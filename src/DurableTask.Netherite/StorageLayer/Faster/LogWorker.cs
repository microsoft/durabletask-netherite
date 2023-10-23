﻿// Copyright (c) Microsoft Corporation.
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
    using System.IO.Hashing;

    class LogWorker : BatchWorker<PartitionUpdateEvent>
    {
        readonly BlobManager blobManager;
        readonly FasterLog log;
        readonly Partition partition;
        readonly StoreWorker storeWorker;
        readonly FasterTraceHelper traceHelper;
        readonly bool traceLogDetails;
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
            this.traceLogDetails = traceHelper.IsTracingAtMostDetailedLevel;

            this.maxFragmentSize = (int) this.blobManager.GetEventLogSettings(partition.Settings.UseSeparatePageBlobStorage, partition.Settings.FasterTuningParameters).PageSize - 64; // faster needs some room for header, 64 bytes is conservative
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

        public TimeSpan? IntakeWorkerProcessingBatchSince => this.intakeWorker.ProcessingBatchSince;

        class IntakeWorker : BatchWorker<PartitionEvent>
        {
            readonly LogWorker logWorker;
            readonly List<PartitionUpdateEvent> updateEvents;

            public IntakeWorker(CancellationToken token, LogWorker logWorker, PartitionTraceHelper traceHelper) : base(nameof(IntakeWorker), true, int.MaxValue, token, traceHelper)
            {
                this.logWorker = logWorker;
                this.updateEvents = new List<PartitionUpdateEvent>();
            }

            protected override Task Process(IList<PartitionEvent> batch)
            {
                if (batch.Count > 0 && !this.logWorker.isShuttingDown)
                {
                    // before processing any update events they need to be serialized
                    // and assigned a commit log position
                    foreach (var evt in batch)
                    {
                        if (evt is PartitionUpdateEvent partitionUpdateEvent)
                        {
                            var bytes = Serializer.SerializeEvent(evt, first | last);
                            this.logWorker.AddToFasterLog(bytes);
                            partitionUpdateEvent.NextCommitLogPosition = this.logWorker.log.TailAddress;
                            this.updateEvents.Add(partitionUpdateEvent);
                        }
                    }

                    // the store worker and the log worker can now process these events in parallel
                    this.logWorker.storeWorker.SubmitBatch(batch);
                    this.logWorker.SubmitBatch(this.updateEvents);

                    this.updateEvents.Clear();
                }

                return Task.CompletedTask;
            }
        }

        public void SubmitEvent(PartitionEvent evt)
        {
            this.intakeWorker.Submit(evt);
        }

        public void SubmitEvents(IList<PartitionEvent> events)
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
                this.AppendToLog(bytes);
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
                    this.AppendToLog(new ReadOnlySpan<byte>(bytes, pos - 1, packetSize));
                    pos += packetSize - 1;
                }
            }
        }

        void AppendToLog(ReadOnlySpan<byte> span)
        {
            this.log.Enqueue(span);
            if (this.traceLogDetails)
            {
                this.TraceLogDetail("Appended", this.log.TailAddress, span);
            }
        }

        void TraceLogDetail(string operation, long nextCommitLogPosition, ReadOnlySpan<byte> span)
        {
            byte firstByte = span[0];
            char f = ((firstByte & first) != 0) ? 'F' : '.';
            char l = ((firstByte & last) != 0) ? 'L' : '.';
            byte[] crc = Crc32.Hash(span);
            this.traceHelper.FasterStorageProgress($"CommitLogEntry {operation} {nextCommitLogPosition} {f}{l} length={span.Length} crc={crc[0]:X2}{crc[1]:X2}{crc[2]:X2}{crc[3]:X2}");
        }

        public async Task PersistAndShutdownAsync()
        {
            this.traceHelper.FasterProgress($"Stopping LogWorker");

            this.isShuttingDown = true;

            await this.intakeWorker.WaitForCompletionAsync();

            await this.WaitForCompletionAsync();

            this.traceHelper.FasterProgress($"Stopped LogWorker");
        }

        protected override async Task Process(IList<PartitionUpdateEvent> batch)
        {
            try
            {
                if (batch.Count > 0)
                {
                    //  checkpoint the log
                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();
                    long previous = this.log.CommittedUntilAddress;

                    await this.log.CommitAsync(); // may commit more events than just the ones in the batch, but that is o.k.

                    this.traceHelper.FasterLogPersisted(this.log.CommittedUntilAddress, batch.Count, (this.log.CommittedUntilAddress - previous), stopwatch.ElapsedMilliseconds);

                    foreach (var evt in batch)
                    {
                        this.LastCommittedInputQueuePosition = Math.Max(this.LastCommittedInputQueuePosition, evt.NextInputQueuePosition);

                        if (!(this.isShuttingDown || this.cancellationToken.IsCancellationRequested))
                        {
                            try
                            {
                                DurabilityListeners.ConfirmDurable(evt);
                            }
                            catch (Exception exception)
                            {
                                // for robustness, swallow exceptions, but report them
                                this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for event {evt} id={evt.EventIdString}", exception, false, false);
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // o.k. during shutdown
            }
            catch (Exception e)
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
                        await iter.WaitAsync(this.cancellationToken);
                    }

                    if (this.traceLogDetails)
                    {
                        this.TraceLogDetail("Read", iter.NextAddress, new ReadOnlySpan<byte>(result, 0, entryLength));
                    }

                    if ((result[0] & first) != none)
                    {
                        if ((result[0] & last) != none)
                        {
                            partitionEvent = TryDeserializePartitionEvent(new MemoryStream(result, 1, entryLength - 1), reassembled:false);                 
                            reassembly = null;
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
                            partitionEvent = TryDeserializePartitionEvent(reassembly, reassembled:true);
                            reassembly = null;
                        }
                    }

                    if (partitionEvent != null)
                    {
                        partitionEvent.NextCommitLogPosition = iter.NextAddress;
                        yield return partitionEvent;
                    }
                }

                PartitionUpdateEvent TryDeserializePartitionEvent(MemoryStream from, bool reassembled)
                {
                    try
                    {
                        return (PartitionUpdateEvent)Serializer.DeserializeEvent(from);
                    }
                    catch (System.Runtime.Serialization.SerializationException serializationException)
                    {
                        string message = $"Could not deserialize partition event (nextCommitLogPosition={iter.NextAddress}, reassembled={reassembled}): {serializationException.Message}";
                        this.blobManager.PartitionErrorHandler.HandleError("ReplayCommitLog", message, serializationException, false, false);
                        return null; // continue even if it leads to incorrect state... as that is still better than no function (retries do not help here)
                    }
                }
            }
        }
    }
}
