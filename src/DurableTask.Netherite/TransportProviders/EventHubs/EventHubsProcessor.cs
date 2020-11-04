// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;

    class EventHubsProcessor : IEventProcessor, TransportAbstraction.IDurabilityListener
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly TaskhubParameters parameters;
        readonly EventHubsTraceHelper traceHelper;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly PartitionContext partitionContext;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly byte[] taskHubGuid;
        readonly uint partitionId;

        //private uint partitionId;
        CancellationTokenSource eventProcessorShutdown;

        // we occasionally checkpoint received packets with eventhubs. It is not required for correctness
        // as we filter duplicates anyway, but it will help startup time.
        readonly Stopwatch timeSinceLastCheckpoint = new Stopwatch();
        volatile Checkpoint pendingCheckpoint;

        // since EventProcessorHost does not redeliver packets, we need to keep them around until we are sure
        // they are processed durably, so we can redeliver them when recycling/recovering a partition
        // we make this a concurrent queue so we can remove confirmed events concurrently with receiving new ones
        readonly ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)> pendingDelivery;
        AsyncLock deliveryLock;

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
            public long NextPacketToReceive;
        }

        readonly Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventHubsProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            PartitionContext partitionContext,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper logger)
        {
            this.host = host;
            this.sender = sender;
            this.parameters = parameters;
            this.pendingDelivery = new ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)>();
            this.partitionContext = partitionContext;
            this.settings = settings;
            this.eventHubName = this.partitionContext.EventHubPath;
            this.eventHubPartition = this.partitionContext.PartitionId;
            this.taskHubGuid = parameters.TaskhubGuid.ToByteArray();
            this.partitionId = uint.Parse(this.eventHubPartition);
            this.traceHelper = logger;
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is opening", this.eventHubName, this.eventHubPartition);
            this.eventProcessorShutdown = new CancellationTokenSource();
            this.deliveryLock = new AsyncLock();

            // we kick off the start-and-retry mechanism for the partition, but don't wait for it to be fully started.
            // instead, we save the task and wait for it when we need it
            this.currentIncarnation = Task.Run(() => this.StartPartitionAsync());

            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} opened", this.eventHubName, this.eventHubPartition);
            return Task.CompletedTask;
        }

        public void ConfirmDurable(Event evt)
        {
            // this is called after an event has committed (i.e. has been durably persisted in the recovery log).
            // so we know we will never need to deliver it again. We remove it from the local buffer, and also checkpoint
            // with EventHubs occasionally.
            while (this.pendingDelivery.TryPeek(out var front) && front.evt.NextInputQueuePosition <= ((PartitionEvent)evt).NextInputQueuePosition)
            {
                if (this.pendingDelivery.TryDequeue(out var candidate))
                {
                    if (this.timeSinceLastCheckpoint.ElapsedMilliseconds > 30000)
                    {
                        this.pendingCheckpoint = new Checkpoint(this.partitionId.ToString(), candidate.offset, candidate.seqno);
                        this.timeSinceLastCheckpoint.Restart();
                    }
                }
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
                    await Task.Delay(-1, prior.ErrorHandler.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                if (!this.eventProcessorShutdown.IsCancellationRequested)
                {
                    // we are now becoming the current incarnation
                    this.currentIncarnation = prior.Next;
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is restarting partition (incarnation {incarnation}) soon", this.eventHubName, this.eventHubPartition, c.Incarnation);
                    await Task.Delay(TimeSpan.FromSeconds(12), this.eventProcessorShutdown.Token).ConfigureAwait(false);
                }
            }

            // check that we are not already shutting down before even starting this
            if (this.eventProcessorShutdown.IsCancellationRequested)
            {
                return null;
            }

            // start the next incarnation, will be on standby until after the current one is terminated
            c.Next = Task.Run(() => this.StartPartitionAsync(c));

            try
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is starting partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);

                // start this partition (which may include waiting for the lease to become available)
                c.Partition = this.host.AddPartition(this.partitionId, this.sender);
                c.NextPacketToReceive = await c.Partition.CreateOrRestoreAsync(c.ErrorHandler, this.parameters.StartPositions[this.partitionId]).ConfigureAwait(false);

                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} started partition (incarnation {incarnation}), next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, c.Incarnation, c.NextPacketToReceive);

                // receive packets already sitting in the buffer; use lock to prevent race with new packets being delivered
                using (await this.deliveryLock.LockAsync())
                {
                    var batch = this.pendingDelivery.Select(triple => triple.Item1).Where(evt => evt.NextInputQueuePosition > c.NextPacketToReceive).ToList();
                    if (batch.Count > 0)
                    {
                        c.NextPacketToReceive = batch[batch.Count - 1].NextInputQueuePosition;
                        c.Partition.SubmitExternalEvents(batch);
                        this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, batch.Count, batch[0].NextInputQueuePosition - 1, c.NextPacketToReceive);
                    }
                }

                this.timeSinceLastCheckpoint.Start();
            }
            catch (OperationCanceledException) when (c.ErrorHandler.IsTerminated)
            {
                // the partition startup was canceled
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} canceled partition startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                c.ErrorHandler.HandleError("EventHubsProcessor.StartPartitionAsync", "failed to start partition", e, true, false);
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} failed during startup (incarnation {incarnation}): {exception}", this.eventHubName, this.eventHubPartition, c.Incarnation, e);
            }

            return c;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is closing", this.eventHubName, this.eventHubPartition);

            this.eventProcessorShutdown.Cancel(); // stops the automatic partition restart

            PartitionIncarnation current = await this.currentIncarnation.ConfigureAwait(false);

            while (current != null && current.ErrorHandler.IsTerminated)
            {
                current = await current.Next.ConfigureAwait(false);
            }

            if (current == null)
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} already canceled or terminated", this.eventHubName, this.eventHubPartition);
            }
            else
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} stopping partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, current.Incarnation);
                await current.Partition.StopAsync(false).ConfigureAwait(false);
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} stopped partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, current.Incarnation);
            }

            await this.SaveEventHubsReceiverCheckpoint(context).ConfigureAwait(false);
            this.deliveryLock.Dispose();

            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} closed", this.eventHubName, this.eventHubPartition);
        }

        async ValueTask SaveEventHubsReceiverCheckpoint(PartitionContext context)
        {
            var checkpoint = this.pendingCheckpoint;
            if (checkpoint != null)
            {
                this.pendingCheckpoint = null;
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is checkpointing receive position through #{seqno}", this.eventHubName, this.eventHubPartition, checkpoint.SequenceNumber);
                try
                {
                    await context.CheckpointAsync(checkpoint).ConfigureAwait(false);
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    // updating EventHubs checkpoints has been known to fail occasionally due to leases shifting around; since it is optional anyway
                    // we don't want this exception to cause havoc
                    this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} failed to checkpoint receive position: {e}", this.eventHubName, this.eventHubPartition, e);
                }
            }
        } 

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception exception)
        {
            this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} received internal error indication from EventProcessorHost: {exception}", this.eventHubName, this.eventHubPartition, exception);
            return Task.CompletedTask;
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {
            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} receiving #{seqno}", this.eventHubName, this.eventHubPartition, packets.First().SystemProperties.SequenceNumber);

            PartitionIncarnation current = await this.currentIncarnation.ConfigureAwait(false);

            while (current != null && current.ErrorHandler.IsTerminated)
            {
                current = await current.Next.ConfigureAwait(false);
            }

            if (current == null)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received packets for closed processor", this.eventHubName, this.eventHubPartition);
                return;
            }

            try
            {
                var batch = new List<PartitionEvent>();
                var receivedTimestamp = current.Partition.CurrentTimeMs;

                using (await this.deliveryLock.LockAsync()) // must prevent rare race with a partition that is currently restarting. Contention is very unlikely.
                {
                    foreach (var eventData in packets)
                    {
                        var seqno = eventData.SystemProperties.SequenceNumber;
                        if (seqno == current.NextPacketToReceive)
                        {
                            PartitionEvent partitionEvent = null;

                            try
                            {
                                Packet.Deserialize(eventData.Body, out partitionEvent, this.taskHubGuid);
                            }
                            catch (Exception)
                            {
                                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} could not deserialize packet #{seqno} ({size} bytes)", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count);
                                throw;
                            }

                            current.NextPacketToReceive = seqno + 1;

                            if (partitionEvent != null)
                            {
                                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {event}", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count, partitionEvent);
                            }
                            else
                            {
                                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} ignored packet #{seqno} for different taskhub", this.eventHubName, this.eventHubPartition, seqno);
                                continue;
                            }

                            partitionEvent.NextInputQueuePosition = current.NextPacketToReceive;
                            batch.Add(partitionEvent);
                            this.pendingDelivery.Enqueue((partitionEvent, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
                            DurabilityListeners.Register(partitionEvent, this);
                            partitionEvent.ReceivedTimestamp = current.Partition.CurrentTimeMs;
                            //partitionEvent.ReceivedTimestampUnixMs = DateTimeOffset.Now.ToUnixTimeMilliseconds();

                            // Output the time it took for the event to go through eventhubs.
                            //if (partitionEvent.SentTimestampUnixMs != 0)
                            //{
                            //    long duration = partitionEvent.ReceivedTimestampUnixMs - partitionEvent.SentTimestampUnixMs;
                            //    this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} eventId={eventId} with {eventHubsLatencyMs} ms latency", this.eventHubName, this.eventHubPartition, seqno, partitionEvent.EventIdString, duration);
                            //}
                        }
                        else if (seqno > current.NextPacketToReceive)
                        {
                            this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received wrong packet, #{seqno} instead of #{expected}", this.eventHubName, this.eventHubPartition, seqno, current.NextPacketToReceive);
                            // this should never happen, as EventHubs guarantees in-order delivery of packets
                            throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                        }
                        else
                        {
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} discarded packet #{seqno} because it is already processed", this.eventHubName, this.eventHubPartition, seqno);
                        }
                    }
                }

                if (batch.Count > 0)
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, batch.Count, batch[0].NextInputQueuePosition - 1, current.NextPacketToReceive);
                    current.Partition.SubmitExternalEvents(batch);
                }

                await this.SaveEventHubsReceiverCheckpoint(context).ConfigureAwait(false);

                // can use this for testing: terminates partition after every one packet received, but
                // that packet is then processed once the partition recovers, so in the end there is progress
                // throw new InvalidOperationException("error injection");
            }
            catch (OperationCanceledException)
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} was terminated", this.eventHubName, this.eventHubPartition);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} encountered an exception while processing packets : {exception}", this.eventHubName, this.eventHubPartition, exception);
                current?.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
            }
        }
    }
}
