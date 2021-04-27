// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using DurableTask.Core.Common;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Channels;
    using FASTER.core;
    using System.Linq;

    class LogWorker : BatchWorker<PartitionUpdateEvent>
    {
        readonly BlobManager blobManager;
        readonly FasterLog log;
        readonly Partition partition;
        readonly StoreWorker storeWorker;
        readonly FasterTraceHelper traceHelper;
        bool isShuttingDown;

        readonly IntakeWorker intakeWorker;

        public LogWorker(BlobManager blobManager, FasterLog log, Partition partition, StoreWorker storeWorker, FasterTraceHelper traceHelper, CancellationToken cancellationToken)
            : base(nameof(LogWorker), true, 500, cancellationToken, partition.TraceHelper)
        {
            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.blobManager = blobManager;
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
            this.traceHelper = traceHelper;
            this.intakeWorker = new IntakeWorker(cancellationToken, this, partition.TraceHelper);

            this.maxFragmentSize = (1 << this.blobManager.EventLogSettings(partition.Settings.UsePremiumStorage).PageSizeBits) - 64; // faster needs some room for header, 64 bytes is conservative
        }

        public const byte first = 0x1;
        public const byte last = 0x2;
        public const byte none = 0x0;
        readonly int maxFragmentSize;

        public void StartProcessing()
        {
            this.intakeWorker.Resume();
            this.Resume();
        }

        public long LastCommittedInputQueuePosition { get; private set; }

        public ConfirmationPromises ConfirmationPromises { get; } = new ConfirmationPromises();

        class IntakeWorker : BatchWorker<PartitionEvent>
        {
            readonly LogWorker logWorker;

            public IntakeWorker(CancellationToken token, LogWorker logWorker, PartitionTraceHelper traceHelper) : base(nameof(IntakeWorker), true, int.MaxValue, token, traceHelper)
            {
                this.logWorker = logWorker;
            }

            protected override Task Process(IList<PartitionEvent> batch)
            {
                if (batch.Count > 0 && !this.logWorker.isShuttingDown)
                {
                    bool notifyLogWorker = false;
                    bool notifyStoreWorker = false;

                    foreach (var evt in batch)
                    {
                        if (evt is PersistenceConfirmationEvent persistenceConfirmationEvent)
                        {
                            uint originPartition = persistenceConfirmationEvent.OriginPartition;
                            long originPosition = persistenceConfirmationEvent.OriginPosition;
                            this.logWorker.traceHelper.FasterProgress($"Received PersistenceConfirmation message: (partition: {originPartition}, position: {originPosition})");
                            this.logWorker.ConfirmationPromises.ResolveConfirmationPromises(originPartition, originPosition);
                        }
                        else
                        {
                            if (evt is PartitionUpdateEvent partitionUpdateEvent)
                            {
                                // if this is event is speculative, register a promise for the completion event
                                if (partitionUpdateEvent is TaskMessagesReceived taskMessagesReceivedEvent 
                                    && taskMessagesReceivedEvent.PersistenceStatus == BatchProcessed.BatchPersistenceStatus.GloballySpeculated)
                                {
                                    this.logWorker.ConfirmationPromises.CreateConfirmationPromise(taskMessagesReceivedEvent);
                                }

                                var bytes = Serializer.SerializeEvent(evt, first | last);
                                this.logWorker.AddToFasterLog(bytes);
                                notifyLogWorker = true;

                                // must set commit log position before processing the event in the storeworker
                                partitionUpdateEvent.NextCommitLogPosition = this.logWorker.log.TailAddress;
                            }

                            this.logWorker.storeWorker.Submit(evt, false);
                            notifyStoreWorker = true;
                        }

                        if (notifyStoreWorker)
                            this.logWorker.storeWorker.Notify();

                        if (notifyLogWorker)
                            this.logWorker.Notify();
                    }
                }

                return Task.CompletedTask;
            }
        }

        public void SubmitInternalEvent(PartitionEvent evt)
        {
            this.intakeWorker.Submit(evt);
        }

        public void SubmitExternalEvents(IList<PartitionEvent> events)
        {
            this.intakeWorker.SubmitBatch(events);
        }

        public void SetLastCheckpointPosition(long commitLogPosition)
        {
            this.traceHelper.FasterProgress($"Truncating FasterLog to {commitLogPosition}");
            this.log.TruncateUntil(commitLogPosition);
        }

        void AddToFasterLog(byte[] bytes)
        {
            if (bytes.Length <= this.maxFragmentSize)
            {
                this.log.Enqueue(bytes);
            }
            else
            {
                // the message is too big. Break it into fragments. 
                int pos = 1;
                while (pos < bytes.Length)
                {
                    bool isLastFragment = 1 + bytes.Length - pos <= this.maxFragmentSize;
                    int packetSize = isLastFragment ? 1 + bytes.Length - pos : this.maxFragmentSize;
                    bytes[pos - 1] = (byte)(((pos == 1) ? first : none) | (isLastFragment ? last : none));
                    this.log.Enqueue(new ReadOnlySpan<byte>(bytes, pos - 1, packetSize));
                    pos += packetSize - 1;
                }
            }
        }

        public async Task PersistAndShutdownAsync()
        {
            this.traceHelper.FasterProgress($"Stopping LogWorker");

            this.isShuttingDown = true;

            await this.intakeWorker.WaitForCompletionAsync().ConfigureAwait(false);

            await this.WaitForCompletionAsync().ConfigureAwait(false);

            this.traceHelper.FasterProgress($"Stopped LogWorker");
        }

        protected override async Task Process(IList<PartitionUpdateEvent> batch)
        {
            try
            {
                if (batch.Count > 0)
                {
                    // Q: Could this be a problem that this here takes a long time possibly blocking
                    //  checkpoint the log
                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();
                    long previous = this.log.CommittedUntilAddress;

                    // Iteratively
                    // - Find the next event that has a dependency (by checking if their Task is set)
                    // - The ones before it can be safely commited.
                    // - For event that is commited we also inform its durability listener
                    // - Wait until the waiting for dependence is complete.
                    // - go back to step 1
                    var lastEnqueuedCommitted = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        var evt = batch[i];
                        
                        if (evt is TaskMessagesReceived taskMessagesReceivedEvent 
                                && taskMessagesReceivedEvent.ConfirmationPromise != null)
                        {
                            // we must commit our log (and send associated confirmations) BEFORE waiting for
                            // dependencies, otherwise we can get stuck in a circular wait-for pattern
                            await this.CommitUntil(batch, lastEnqueuedCommitted, i);

                            // Progress the last commited index
                            lastEnqueuedCommitted = i;

                            // Before continuing, wait for the dependencies of this update to be done, so that we can continue
                            await taskMessagesReceivedEvent.ConfirmationPromise.Task;
                        }
                    }
                    await this.CommitUntil(batch, lastEnqueuedCommitted, batch.Count);
                }
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // o.k. during shutdown
            }
            catch (Exception e) when (!(e is OutOfMemoryException))
            {
                this.partition.ErrorHandler.HandleError("LogWorker.Process", "Encountered exception while working on commit log", e, true, false);
            }
        }

        public async Task ReplayCommitLog(long from, StoreWorker worker)
        {
            // this procedure is called by StoreWorker during recovery. It replays all the events
            // that were committed to the log but are not reflected in the loaded store checkpoint.
            try
            {
                // we create a pipeline where the fetch task obtains a stream of events and then duplicates the
                // stream, so it can get replayed and prefetched in parallel.
                var prefetchChannel = Channel.CreateBounded<TrackedObjectKey>(1000);
                var replayChannel = Channel.CreateBounded<PartitionUpdateEvent>(1000);

                var fetchTask = this.FetchEvents(from, replayChannel.Writer, prefetchChannel.Writer);
                var replayTask = Task.Run(() => this.ReplayEvents(replayChannel.Reader, worker));
                var prefetchTask = Task.Run(() => worker.RunPrefetchSession(prefetchChannel.Reader.ReadAllAsync(this.cancellationToken)));

                await fetchTask;
                await replayTask;
                // note: we are not awaiting the prefetch task since completing the prefetches is not essential to continue.
            }
            catch (Exception exception)
                when (this.cancellationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.partition.ErrorHandler.Token);
            }
        }

        async Task FetchEvents(long from, ChannelWriter<PartitionUpdateEvent> replayChannelWriter, ChannelWriter<TrackedObjectKey> prefetchChannelWriter)
        {
            await foreach (var partitionEvent in this.EventsToReplay(from))
            {
                this.cancellationToken.ThrowIfCancellationRequested();

                await replayChannelWriter.WriteAsync(partitionEvent);

                if (partitionEvent is IRequiresPrefetch evt)
                {
                    foreach (var key in evt.KeysToPrefetch)
                    {
                        await prefetchChannelWriter.WriteAsync(key);
                    }
                }
            }

            replayChannelWriter.Complete();
            prefetchChannelWriter.Complete();
        }

        async Task ReplayEvents(ChannelReader<PartitionUpdateEvent> reader, StoreWorker worker)
        {
            await foreach (var partitionEvent in reader.ReadAllAsync(this.cancellationToken))
            {
                await worker.ReplayUpdate(partitionEvent);
            }
        }

        async IAsyncEnumerable<PartitionUpdateEvent> EventsToReplay(long from)
        {
            long to = this.log.TailAddress;
            using (FasterLogScanIterator iter = this.log.Scan(from, to))
            {
                byte[] result;
                int entryLength;
                long currentAddress;
                MemoryStream reassembly = null;

                while (!this.cancellationToken.IsCancellationRequested)
                {
                    PartitionUpdateEvent partitionEvent = null;

                    while (!iter.GetNext(out result, out entryLength, out currentAddress))
                    {
                        if (currentAddress >= to)
                        {
                            yield break;
                        }
                        await iter.WaitAsync(this.cancellationToken).ConfigureAwait(false);
                    }

                    if ((result[0] & first) != none)
                    {
                        if ((result[0] & last) != none)
                        {
                            partitionEvent = (PartitionUpdateEvent)Serializer.DeserializeEvent(new ArraySegment<byte>(result, 1, entryLength - 1));
                        }
                        else
                        {
                            reassembly = new MemoryStream();
                            reassembly.Write(result, 1, entryLength - 1);
                        }
                    }
                    else
                    {
                        reassembly.Write(result, 1, entryLength - 1);

                        if ((result[0] & last) != none)
                        {
                            reassembly.Position = 0;
                            partitionEvent = (PartitionUpdateEvent)Serializer.DeserializeEvent(reassembly);
                            reassembly = null;
                        }
                    }

                    if (partitionEvent != null)
                    {
                        partitionEvent.NextCommitLogPosition = iter.NextAddress;
                        yield return partitionEvent;
                    }
                }
            }
        }

        async Task CheckpointLog(int count, long latestConsistentAddress)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            long previous = this.log.CommittedUntilAddress;

            this.blobManager.BeginCommit(latestConsistentAddress);
            await this.log.CommitAndWaitUntil(latestConsistentAddress);
            this.blobManager.EndCommit();

            this.traceHelper.FasterLogPersisted(this.log.CommittedUntilAddress, count, (this.log.CommittedUntilAddress - previous), stopwatch.ElapsedMilliseconds);
        }


        async Task CommitUntil(IList<PartitionUpdateEvent> batch, int from, int to)
        {
            var count = to - from;
            if (count > 0)
            {
                var latestConsistentAddress = batch[to - 1].NextCommitLogPosition;

                await this.CheckpointLog(count, latestConsistentAddress);

                // Now that the log is commited, we can send persistence confirmation events for
                // the commited events.
                //
                // TODO: (Optimization) Can we group and only send aggregate persistence confirmations?
                //       This could relieve the pressure on sending/receiving from Eventhubs

                for (var j = from; j < to; j++)
                {
                    var currEvt = batch[j];

                    this.LastCommittedInputQueuePosition = Math.Max(this.LastCommittedInputQueuePosition, currEvt.NextInputQueuePosition);
                   
                    // Q: Is this the right place to check for that, or should we do it earlier?
                    if (!(this.isShuttingDown || this.cancellationToken.IsCancellationRequested))
                    {

                        try
                        {
                            DurabilityListeners.ConfirmDurable(currEvt);
                            // Possible optimization: Move persistence confirmation logic to the lower level and out of the application layer
                        }
                        catch (Exception exception) when (!(exception is OutOfMemoryException))
                        {
                            // for robustness, swallow exceptions, but report them
                            this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for event {currEvt} id={currEvt.EventIdString}", exception, false, false);
                        }
                    }
                }
            }
        }
    }
}
