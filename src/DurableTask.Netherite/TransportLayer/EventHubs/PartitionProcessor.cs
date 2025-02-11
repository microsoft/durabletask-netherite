// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using EventPosition = Azure.Messaging.EventHubs.Consumer.EventPosition;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Extensions.Logging;
    using Azure.Messaging.EventHubs.Processor;

    /// <summary>
    /// Delivers events targeting a specific Netherite partition.
    /// </summary>
    class PartitionProcessor : IEventProcessor, TransportAbstraction.IDurabilityListener
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly TaskhubParameters parameters;
        readonly EventHubsTraceHelper traceHelper;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly EventHubsTransport eventHubsTransport;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly byte[] taskHubGuid;
        readonly uint partitionId;
        readonly BlobBatchReceiver<PartitionEvent> blobBatchReceiver;
        readonly EventProcessorClient client;
        readonly TaskCompletionSource<long> firstPacketToReceive; 
        readonly CancellationTokenSource shutdownSource;
        readonly CancellationTokenRegistration? shutdownRegistration;

        // we set this task once shutdown has been initiated
        Task shutdownTask = null;

        // we continuously validate the sequence numbers that are being delivered
        long nextToReceive;

        // since EventProcessorHost does not redeliver packets, we need to keep them around until we are sure
        // they are processed durably, so we can redeliver them when recycling/recovering a partition
        // we make this a concurrent queue so we can remove confirmed events concurrently with receiving new ones
        readonly ConcurrentQueue<EventEntry> pendingDelivery;
        AsyncLock deliveryLock;


        record struct EventEntry(long SeqNo, long BatchPos, PartitionEvent Event, bool LastInBatch, BlockBlobClient BatchBlob)
        {
            public EventEntry(long seqNo, long batchPos, PartitionEvent evt) : this(seqNo, batchPos, evt, false, null) { }

            public EventEntry(long seqNo, long batchPos, PartitionEvent evt, BlockBlobClient blob) : this(seqNo, batchPos, evt, true, blob) { }
        }

        // this points to the latest incarnation of this partition; it gets
        // updated as we recycle partitions (create new incarnations after failures)
        volatile Task<PartitionIncarnation> currentIncarnation;

        /// <summary>
        /// The event processor can recover after exceptions, so we encapsulate
        /// the currently active partition
        /// </summary>
        class PartitionIncarnation
        {
            public int Incarnation;
            public IPartitionErrorHandler ErrorHandler;
            public TransportAbstraction.IPartition Partition;
            public Task<PartitionIncarnation> Next;
            public (long seqNo, int batchPos) NextPacketToReceive;
            public int SuccessiveStartupFailures;
        }

        readonly Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public PartitionProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            EventProcessorClient client,
            string partitionId,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTransport eventHubsTransport,
            EventHubsTraceHelper traceHelper,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.sender = sender;
            this.parameters = parameters;
            this.pendingDelivery = new();
            this.settings = settings;
            this.eventHubsTransport = eventHubsTransport;
            this.client = client;
            this.eventHubName = client.EventHubName;
            this.eventHubPartition = partitionId;
            this.taskHubGuid = parameters.TaskhubGuid.ToByteArray();
            this.partitionId = uint.Parse(this.eventHubPartition);
            this.traceHelper = new EventHubsTraceHelper(traceHelper, this.partitionId);
            this.shutdownSource = new CancellationTokenSource();
            string traceContext = $"EventHubsProcessor {this.eventHubName}/{this.eventHubPartition}";
            this.blobBatchReceiver = new BlobBatchReceiver<PartitionEvent>(traceContext, this.traceHelper, this.settings);
            this.firstPacketToReceive = new TaskCompletionSource<long>();

            this.shutdownRegistration = shutdownToken.Register(() => this.IdempotentShutdown("EventHubsProcessor.shutdownToken", eventHubsTransport.FatalExceptionObserved));
        }

        async Task<EventPosition> IEventProcessor.OpenAsync(CancellationToken cancellationToken)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} OpenAsync called", this.eventHubName, this.eventHubPartition);
            this.deliveryLock = new AsyncLock(this.shutdownSource.Token);

            try
            {
                // we kick off the start-and-retry mechanism for the partition incarnations
                this.currentIncarnation = Task.Run(() => this.StartPartitionAsync());

                // if the cancellation token fires, shut everything down
                // this must be registered AFTER the current incarnation is assigned
                // since it may fire immediately if already canceled at this point
                using var _ = cancellationToken.Register(() => this.IdempotentShutdown("PartitionProcessor.OpenAsync cancellationToken", true));

                // then we wait for a successful partition creation or recovery so we can tell where to resume packet processing
                var firstPacketToReceive = await this.firstPacketToReceive.Task;

                this.nextToReceive = firstPacketToReceive;

                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} OpenAsync returned, first packet to receive is #{seqno}", this.eventHubName, this.eventHubPartition, firstPacketToReceive);

                return EventPosition.FromSequenceNumber(firstPacketToReceive - 1, isInclusive: false);
            }
            catch (OperationCanceledException) when (this.shutdownSource.IsCancellationRequested)
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} OpenAsync canceled by shutdown", this.eventHubName, this.eventHubPartition);

                return EventPosition.Latest; // does not matter since we are already shut down
            }
        }

        public void ConfirmDurable(Event evt)
        {          
            if (this.shutdownSource.IsCancellationRequested)
            {
                return;
            }

            List<BlockBlobClient> obsoleteBatches = null;

            if (this.traceHelper.IsEnabled(LogLevel.Trace))
            {
                this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} ConfirmDurable nextInputQueuePosition={nextInputQueuePosition}", this.eventHubName, this.eventHubPartition, ((PartitionEvent)evt).NextInputQueuePositionTuple);
            }

            // this is called after an event has committed (i.e. has been durably persisted in the recovery log).
            // so we know we will never need to deliver it again. We remove it from the local buffer
            // and delete the blob batch if this was the last event in the batch.
            while (this.pendingDelivery.TryPeek(out var front) 
                && (front.Event == evt || front.Event.NextInputQueuePositionTuple.CompareTo(((PartitionEvent) evt).NextInputQueuePositionTuple) < 0))
            {
                if (this.pendingDelivery.TryDequeue(out var confirmed) && front == confirmed)
                {
                    if (this.traceHelper.IsEnabled(LogLevel.Trace))
                    {
                        this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} discarding buffered event nextInputQueuePosition={nextInputQueuePosition} lastInBatch={lastInBatch} seqno={seqNo} batchBlob={batchBlob}", this.eventHubName, this.eventHubPartition, front.Event.NextInputQueuePositionTuple, confirmed.LastInBatch, confirmed.SeqNo, confirmed.BatchBlob?.Name);
                    }

                    if (confirmed.LastInBatch)
                    {
                        if (confirmed.BatchBlob != null)
                        {
                            (obsoleteBatches ??= new()).Add(confirmed.BatchBlob);
                        }
                    }
                }
            }

            if (obsoleteBatches != null)
            {
                Task backgroundTask = Task.Run(() => this.blobBatchReceiver.DeleteBlobBatchesAsync(obsoleteBatches));
            }
        }

        async Task<PartitionIncarnation> StartPartitionAsync(PartitionIncarnation prior = null)
        {
            // create the record for this incarnation
            var c = new PartitionIncarnation()
            {
                Incarnation = (prior != null) ? (prior.Incarnation + 1) : 1,
                ErrorHandler = this.host.CreateErrorHandler(this.partitionId),
            };

            // if this is not the first incarnation, stay on standby until the previous incarnation is terminated.
            if (c.Incarnation > 1)
            {
                try
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is readying next startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);

                    await Task.Delay(-1, prior.ErrorHandler.Token);
                }
                catch (OperationCanceledException)
                {
                }

                if (!this.shutdownSource.IsCancellationRequested)
                {
                    // we are now becoming the current incarnation
                    this.currentIncarnation = prior.Next;

                    // sometimes we can get stuck into a loop of failing attempts to reincarnate a partition. 
                    // We don't want to waste CPU  and pollute the logs, but we also can't just give up because
                    // the failures can be transient. Thus we back off the retry pace.
                    TimeSpan addedDelay = 
                          (prior.SuccessiveStartupFailures < 2)   ? TimeSpan.Zero
                        : (prior.SuccessiveStartupFailures < 10)  ? TimeSpan.FromSeconds(1)
                        : (prior.SuccessiveStartupFailures < 100) ? TimeSpan.FromSeconds(5)
                        : (prior.SuccessiveStartupFailures < 200) ? TimeSpan.FromSeconds(10)
                                                                  : TimeSpan.FromMinutes(1);


                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is restarting partition (incarnation {incarnation}) soon; addedDelay={addedDelay}", this.eventHubName, this.eventHubPartition, c.Incarnation, addedDelay);

                    if (addedDelay != TimeSpan.Zero)
                    {
                        await Task.Delay(addedDelay);
                    }

                    // the previous incarnation has already been terminated. But it may not have cleaned up yet.
                    // We wait (but no more than 20 seconds) for the previous partition to dispose its assets
                    bool disposalComplete = prior.ErrorHandler.WaitForDisposeTasks(TimeSpan.FromSeconds(20));

                    if (!disposalComplete)
                    {
                        this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} partition (incarnation {incarnation}) timed out waiting for disposal of previous incarnation", this.eventHubName, this.eventHubPartition, c.Incarnation);
                    }
                }
            }

            // check that we are not already shutting down before even starting this
            if (this.shutdownSource.IsCancellationRequested)
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is cancelling startup of incarnation {incarnation}", this.eventHubName, this.eventHubPartition, c.Incarnation);
                return null;
            }

            // start the next incarnation task, will be on standby until after the current one is terminated
            c.Next = this.StartPartitionAsync(c);

            try
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is starting partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);

                // to handle shutdown before startup completes, register a force-termination
                using var registration = this.shutdownSource.Token.Register(
                    () => c.ErrorHandler.HandleError(
                        nameof(StartPartitionAsync),
                        "EventHubsProcessor shut down before partition fully started",
                        null,
                        terminatePartition: true,
                        reportAsWarning: true));

                // start this partition (which may include waiting for the lease to become available)
                c.Partition = this.host.AddPartition(this.partitionId, this.sender);
                c.NextPacketToReceive = await c.Partition.CreateOrRestoreAsync(c.ErrorHandler, this.parameters, this.eventHubsTransport.Fingerprint);

                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} started partition (incarnation {incarnation}), next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, c.Incarnation, c.NextPacketToReceive);

                if (!this.firstPacketToReceive.TrySetResult(c.NextPacketToReceive.seqNo))
                {
                    this.shutdownSource.Token.ThrowIfCancellationRequested();

                    // this is not the first incarnation so there may be some packets that were received earlier and are sitting in the buffer;
                    // use lock to prevent race with new packets being delivered
                    using (await this.deliveryLock.LockAsync())
                    {
                        this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} checking for packets requiring redelivery (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
                        var batch = this.pendingDelivery
                            .Select(x => x.Event)
                            .Where(evt => (evt.NextInputQueuePosition, evt.NextInputQueueBatchPosition).CompareTo(c.NextPacketToReceive) > 0)
                            .ToList();
                        if (batch.Count > 0)
                        {
                            var lastInBatch = batch[batch.Count - 1];
                            c.NextPacketToReceive = (lastInBatch.NextInputQueuePosition, lastInBatch.NextInputQueueBatchPosition);
                            c.Partition.SubmitEvents(batch);
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} redelivered {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno} (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, batch.Count, batch[0].NextInputQueuePosition - 1, c.NextPacketToReceive, c.Incarnation);
                        }
                        else
                        {
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} has no packets requiring redelivery ", this.eventHubName, this.eventHubPartition);
                        }
                    }
                } 

                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} completed partition startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
            }
            catch (OperationCanceledException) when (c.ErrorHandler.IsTerminated || this.shutdownSource.Token.IsCancellationRequested)
            {
                // the partition startup was canceled
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} canceled partition startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
            }
            catch (Exception e)
            {
                c.SuccessiveStartupFailures = 1 + (prior?.SuccessiveStartupFailures ?? 0);
                c.ErrorHandler.HandleError("EventHubsProcessor.StartPartitionAsync", "failed to start partition", e, true, false);
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} failed during startup (incarnation {incarnation}): {exception}", this.eventHubName, this.eventHubPartition, c.Incarnation, e);
            }        

            return c;
        }

        void IdempotentShutdown(string reason, bool quickly)
        {
            var waitForConfirmation = new TaskCompletionSource<bool>();
            var task = ShutdownAsync();
            var originalValue = Interlocked.CompareExchange(ref this.shutdownTask, task, null);
            bool isFirst = (originalValue == null);

            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} triggered shutdown (reason: {reason}, quickly: {quickly}, isFirst:{isFirst})", this.eventHubName, this.eventHubPartition, reason, quickly, isFirst);

            waitForConfirmation.SetResult(isFirst); // only the first shutdown should proceed

            async Task ShutdownAsync()
            {
                if (await waitForConfirmation.Task) // only the first shutdown should proceed
                {
                    try
                    {
                        this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is shutting down (reason: {reason}, quickly: {quickly})", this.eventHubName, this.eventHubPartition, reason, quickly);

                        this.shutdownSource.Cancel(); // stops all reincarnations, among other things

                        this.firstPacketToReceive.TrySetCanceled(); // cancel partition opening if we are still waiting for it

                        PartitionIncarnation current = await this.currentIncarnation;

                        while (current != null && current.ErrorHandler.IsTerminated)
                        {
                            current = await current.Next;
                        }

                        if (current == null)
                        {
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} already canceled or terminated", this.eventHubName, this.eventHubPartition);
                        }
                        else
                        {
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} stopping partition (incarnation: {incarnation}, quickly: {quickly})", this.eventHubName, this.eventHubPartition, current.Incarnation, quickly);
                            await current.Partition.StopAsync(quickly);
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} stopped partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, current.Incarnation);
                        }

                        this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} successfully shut down", this.eventHubName, this.eventHubPartition);
                    }
                    catch (Exception exception)
                    {
                        this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} failed to shut down: {exception})", this.eventHubName, this.eventHubPartition, exception);
                    }
                    finally
                    {
                        this.deliveryLock.Dispose();
                    }
                }
            }
        }

        async Task IEventProcessor.CloseAsync(ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} CloseAsync called (reason: {reason})", this.eventHubName, this.eventHubPartition, reason);

            this.IdempotentShutdown("CloseAsync", reason == ProcessingStoppedReason.OwnershipLost);

            await this.shutdownTask;

            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} CloseAsync returned", this.eventHubName, this.eventHubPartition);
        }

        async Task IEventProcessor.ProcessErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            if (exception is OperationCanceledException && this.shutdownSource.IsCancellationRequested)
            {
                // normal to see some cancellations during shutdown
            }

            if (exception is Azure.Messaging.EventHubs.EventHubsException eventHubsException)
            {
                switch (eventHubsException.Reason)
                {
                    case EventHubsException.FailureReason.ResourceNotFound:
                        // occurs when partition hubs was deleted either accidentally, or intentionally after messages were lost due to the retention limit
                        this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} EventHub was deleted, initiating recovery via restart", this.eventHubName, this.eventHubPartition);
                        await this.eventHubsTransport.ExitProcess(false);
                        return;

                    case EventHubsException.FailureReason.ConsumerDisconnected:
                        // since this processor is no longer going to receive events, let's shut it down 
                        this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received ConsumerDisconnected notification", this.eventHubName, this.eventHubPartition);
                        this.IdempotentShutdown("Receiver was disconnected", true);
                        return;

                    case EventHubsException.FailureReason.InvalidClientState:
                        // something is permantently broken inside EH client, let's try to recover via restart 
                        this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received InvalidClientState notification, initiating recovery via restart", this.eventHubName, this.eventHubPartition);
                        await this.eventHubsTransport.ExitProcess(false);
                        return;
                }
            }

            this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} received internal error indication from EventProcessorHost: {exception}", this.eventHubName, this.eventHubPartition, exception);
        }

        async Task IEventProcessor.ProcessEventBatchAsync(IEnumerable<EventData> packets, CancellationToken cancellationToken)
        {
            if (this.shutdownSource.IsCancellationRequested)
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} ProcessEventBatchAsync canceled (already shut down)", this.eventHubName, this.eventHubPartition);
                return;
            }

            // if the cancellation token fires, shut everything down
            using var _ = cancellationToken.Register(() => this.IdempotentShutdown("ProcessEventBatchAsync cancellationToken", true));

            EventData first = packets.FirstOrDefault();

            if (first == null)
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received empty batch", this.eventHubName, this.eventHubPartition);
                return;
            }

            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is receiving events starting with #{seqno}", this.eventHubName, this.eventHubPartition, first.SequenceNumber);
 
            if (first.SequenceNumber > this.nextToReceive)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} missing packets in sequence, #{seqno} instead of #{expected}. Initiating recovery via delete and restart.", this.eventHubName, this.eventHubPartition, first.SequenceNumber, this.nextToReceive);
                await this.eventHubsTransport.ExitProcess(true);
                return;
            }
            else if (first.SequenceNumber < this.nextToReceive)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} duplicate packet in sequence, #{seqno} instead of #{expected}.", this.eventHubName, this.eventHubPartition, first.SequenceNumber, this.nextToReceive);
            }

            PartitionIncarnation current = await this.currentIncarnation;

            while (current != null && current.ErrorHandler.IsTerminated)
            {
                current = await current.Next;
            }

            if (current == null)
            {
                this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} received packets for closed processor, discarded", this.eventHubName, this.eventHubPartition);
            }
            else
            {
                this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} is delivering to incarnation {seqno}", this.eventHubName, this.eventHubPartition, current.Incarnation);
            }

            try
            {
                var receivedTimestamp = current.Partition.CurrentTimeMs;
                int totalEvents = 0;
                Stopwatch stopwatch = Stopwatch.StartNew();

                this.shutdownSource.Token.ThrowIfCancellationRequested();

                using (await this.deliveryLock.LockAsync()) // must prevent rare race with a partition that is currently restarting. Contention is very unlikely.
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) is processing packets", this.eventHubName, this.eventHubPartition, current.Incarnation);

                    // we need to update the next expected position tuple (seqno,batchpos) even if the iterator returns nothing, since it may have discarded some packets.
                    // iterators do not support ref arguments, so we define a Position object with mutable fields to work around this limitation
                    Position nextPacketToReceive = new Position() { SeqNo = current.NextPacketToReceive.seqNo, BatchPos = current.NextPacketToReceive.batchPos };

                    await foreach ((EventData eventData, PartitionEvent[] events, long seqNo, BlockBlobClient blob) in this.blobBatchReceiver.ReceiveEventsAsync(this.taskHubGuid, packets, this.shutdownSource.Token, current.ErrorHandler, nextPacketToReceive))
                    {
                        int numSkipped = 0;

                        for (int i = 0; i < events.Length; i++)
                        {
                            PartitionEvent evt = events[i];

                            if (evt == null)
                            {
                                numSkipped++;
                                continue; // was skipped over by the batch receiver because it is already processed
                            }

                            if (i < events.Length - 1) // this is not the last event in the batch
                            {
                                // the next input queue position is the next position within the same batch
                                evt.NextInputQueuePosition = seqNo;
                                evt.NextInputQueueBatchPosition = i + 1;
                                this.pendingDelivery.Enqueue(new EventEntry(seqNo, i, evt));
                            }
                            else // this is the last event in the batch
                            {
                                // the next input queue position is the first entry of of the next batch
                                evt.NextInputQueuePosition = seqNo + 1;
                                evt.NextInputQueueBatchPosition = 0;
                                this.pendingDelivery.Enqueue(new EventEntry(seqNo, i, evt, blob));
                            }

                            if (this.traceHelper.IsEnabled(LogLevel.Trace))
                            {
                                this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) received packet #({seqno},{subSeqNo}) {event} id={eventId}", this.eventHubName, this.eventHubPartition, current.Incarnation, seqNo, i, evt, evt.EventIdString);
                            }

                            if (evt is PartitionUpdateEvent partitionUpdateEvent)
                            {
                                DurabilityListeners.Register(evt, this);
                            }

                            totalEvents++;
                        }

                        current.Partition.SubmitEvents(numSkipped == 0 ? events : events.Skip(numSkipped).ToList());

                        this.nextToReceive = eventData.SequenceNumber + 1;
                    }

                    current.NextPacketToReceive = (nextPacketToReceive.SeqNo, nextPacketToReceive.BatchPos);
                }

                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) received {totalEvents} events in {latencyMs:F2}ms, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, current.Incarnation, totalEvents, stopwatch.Elapsed.TotalMilliseconds, first.SequenceNumber, current.NextPacketToReceive);
            }
            catch (OperationCanceledException) when (this.shutdownSource.IsCancellationRequested) // we should only ignore these exceptions during eventProcessor shutdowns. See :  https://github.com/microsoft/durabletask-netherite/pull/347
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) was terminated", this.eventHubName, this.eventHubPartition, current.Incarnation);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) ProcessEventsAsync encountered an exception while processing packets : {exception}", this.eventHubName, this.eventHubPartition, current.Incarnation, exception);
                current?.ErrorHandler.HandleError("PartitionProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
                this.nextToReceive = packets.LastOrDefault().SequenceNumber;
            }
        }
    }
}
