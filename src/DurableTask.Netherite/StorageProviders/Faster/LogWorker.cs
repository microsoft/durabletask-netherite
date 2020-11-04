// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Faster
{
    using DurableTask.Core.Common;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

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
            : base(nameof(LogWorker), true, cancellationToken)
        {
            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.blobManager = blobManager;
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
            this.traceHelper = traceHelper;
            this.intakeWorker = new IntakeWorker(cancellationToken, this);

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

        class IntakeWorker : BatchWorker<PartitionEvent>
        {
            readonly LogWorker logWorker;
            readonly List<PartitionUpdateEvent> updateEvents;

            public IntakeWorker(CancellationToken token, LogWorker logWorker) : base(nameof(IntakeWorker), true, token)
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

            protected override void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
            {
                this.logWorker.traceHelper.FasterProgress($"IntakeWorker completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds} nextBatch={nextBatch}");
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
                    //  checkpoint the log
                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();
                    long previous = this.log.CommittedUntilAddress;

                    await this.log.CommitAsync().ConfigureAwait(false); // may commit more events than just the ones in the batch, but that is o.k.

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
                            catch (Exception exception) when (!(exception is OutOfMemoryException))
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
            catch (Exception e) when (!(e is OutOfMemoryException))
            {
                this.partition.ErrorHandler.HandleError("LogWorker.Process", "Encountered exception while working on commit log", e, true, false);
            }        
        }

        protected override void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
            this.traceHelper.FasterProgress($"LogWorker completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds} nextBatch={nextBatch}");
        }

        public async Task ReplayCommitLog(long from, StoreWorker worker)
        {
            // this procedure is called by StoreWorker during recovery. It replays all the events
            // that were committed to the log but are not reflected in the loaded store checkpoint.
            try
            {
                var to = this.log.TailAddress;

                using (var iter = this.log.Scan((long)from, to))
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
                                return;
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
                            await worker.ReplayUpdate(partitionEvent).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception exception)
                when (this.cancellationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.partition.ErrorHandler.Token);
            }
        }
    }
}
