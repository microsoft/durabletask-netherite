// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using DurableTask.Netherite.Scaling;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    class StoreWorker : BatchWorker<PartitionEvent>
    {
        readonly TrackedObjectStore store;
        readonly Partition partition;
        readonly FasterTraceHelper traceHelper;
        readonly BlobManager blobManager;
        readonly Random random;
        readonly ReplayChecker replayChecker;

        readonly EffectTracker effectTracker;

        bool isShuttingDown;

        public string InputQueueFingerprint { get; private set; }
        public long InputQueuePosition { get; private set; }
        public long CommitLogPosition { get; private set; }

        public LogWorker LogWorker { get; set; }

        // periodic index and store checkpointing
        CheckpointTrigger pendingCheckpointTrigger;
        Task pendingIndexCheckpoint;
        Task<(long, long)> pendingStoreCheckpoint;
        long lastCheckpointedInputQueuePosition;
        long lastCheckpointedCommitLogPosition;
        long numberEventsSinceLastCheckpoint;
        DateTime timeOfNextIdleCheckpoint;

        // periodic compaction
        Task<long?> pendingCompaction;

        // periodic load publishing
        PartitionLoadInfo loadInfo;
        DateTime lastPublished;
        string lastPublishedLatencyTrend;
        int lastPublishedCachePct;
        public static TimeSpan PublishInterval = TimeSpan.FromSeconds(8);
        public static TimeSpan PokePeriod = TimeSpan.FromSeconds(3); // allows storeworker to checkpoint and publish load even while idle

        CancellationTokenSource ioCompletionNotificationCancellation;


        public StoreWorker(TrackedObjectStore store, Partition partition, FasterTraceHelper traceHelper, BlobManager blobManager, CancellationToken cancellationToken)
            : base($"{nameof(StoreWorker)}{partition.PartitionId:D2}", true, 500, cancellationToken, partition.TraceHelper)
        {
            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.store = store;
            this.partition = partition;
            this.traceHelper = traceHelper;
            this.blobManager = blobManager;
            this.random = new Random();
            this.replayChecker = this.partition.Settings.TestHooks?.ReplayChecker;

            this.loadInfo = PartitionLoadInfo.FirstFrame(this.partition.Settings.WorkerId);
            this.lastPublished = DateTime.MinValue;
            this.lastPublishedLatencyTrend = "";
            this.lastPublishedCachePct = 0;

            // construct an effect tracker that we use to apply effects to the store
            this.effectTracker = new TrackedObjectStoreEffectTracker(this.partition, this, store);
        }

        public async Task Initialize(long initialCommitLogPosition, string fingerprint)
        {
            this.partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.InputQueueFingerprint = fingerprint;
            this.InputQueuePosition = 0;
            this.CommitLogPosition = initialCommitLogPosition;
           
            this.store.InitMainSession();

            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await this.store.CreateAsync(key);
                target.OnFirstInitialization();
            }

            this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
            this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
            this.numberEventsSinceLastCheckpoint = initialCommitLogPosition;

            this.replayChecker?.PartitionStarting(this.partition, this.store, this.CommitLogPosition, this.InputQueuePosition);
        }

        public void StartProcessing()
        {
            this.Resume();
            var pokeLoop = this.PokeLoop();
        }

        public void SetCheckpointPositionsAfterRecovery(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint)
        {
            this.CommitLogPosition = commitLogPosition;
            this.InputQueuePosition = inputQueuePosition;
            this.InputQueueFingerprint = inputQueueFingerprint;

            this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
            this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
            this.numberEventsSinceLastCheckpoint = 0;
            this.ScheduleNextIdleCheckpointTime();
        }

        internal async ValueTask TakeFullCheckpointAsync(string reason)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            if (this.store.TakeFullCheckpoint(this.CommitLogPosition, this.InputQueuePosition, this.InputQueueFingerprint, out var checkpointGuid))
            {
                this.traceHelper.FasterCheckpointStarted(checkpointGuid, reason, this.store.StoreStats.Get(), this.CommitLogPosition, this.InputQueuePosition);

                // do the faster full checkpoint and then finalize it
                await this.store.CompleteCheckpointAsync();
                await this.store.FinalizeCheckpointCompletedAsync(checkpointGuid);

                this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
                this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
                this.numberEventsSinceLastCheckpoint = 0;

                this.traceHelper.FasterCheckpointPersisted(checkpointGuid, reason, this.CommitLogPosition, this.InputQueuePosition, stopwatch.ElapsedMilliseconds);
            }
            else
            {
                this.traceHelper.FasterProgress($"Checkpoint skipped: {reason}");
            }

            this.ScheduleNextIdleCheckpointTime();
        }

        public async Task CancelAndShutdown()
        {
            this.traceHelper.FasterProgress("Stopping StoreWorker");

            this.isShuttingDown = true;

            await this.WaitForCompletionAsync();

            // wait for any in-flight checkpoints. It is unlikely but not impossible.
            if (this.pendingIndexCheckpoint != null)
            {
                await this.pendingIndexCheckpoint;
            }
            if (this.pendingStoreCheckpoint != null)
            {
                await this.pendingStoreCheckpoint;
            }

            this.replayChecker?.PartitionStopped(this.partition);

            this.traceHelper.FasterProgress("Stopped StoreWorker");
        }

        public Task RunPrefetchSession(IAsyncEnumerable<TrackedObjectKey> keys)
        {
            return this.store.RunPrefetchSession(keys);
        }

        async ValueTask PublishLoadAndPositions()
        {
            bool publishLoadInfo = await this.UpdateLoadInfo();

            // take the current load info and put the next frame in its place
            var loadInfoToPublish = this.loadInfo;
            this.loadInfo = loadInfoToPublish.NextFrame();

            this.lastPublishedLatencyTrend = loadInfoToPublish.LatencyTrend;
            this.lastPublishedCachePct = loadInfoToPublish.CachePct;

            this.partition.TraceHelper.TracePartitionLoad(loadInfoToPublish);

            var positionsReceived = await this.CollectSendAndReceivePositions();

            // to avoid publishing not-yet committed state, publish
            // only after the current log is persisted.
            if (publishLoadInfo || positionsReceived != null)
            {
                var task = this.LogWorker.WaitForCompletionAsync()
                    .ContinueWith((t) =>
                    {
                        if (publishLoadInfo)
                        {
                            this.partition.LoadPublisher?.Submit((this.partition.PartitionId, loadInfoToPublish));
                        }
                        if (positionsReceived != null)
                        {
                            this.partition.Send(positionsReceived);
                        }
                    });
            }
        }

        async ValueTask<bool> UpdateLoadInfo()
        {
            foreach (var k in TrackedObjectKey.GetSingletons())
            {
                (await this.store.ReadAsync(k, this.effectTracker)).UpdateLoadInfo(this.loadInfo);
            }

            this.loadInfo.MissRate = this.store.StoreStats.GetMissRate();
            (this.loadInfo.CacheMB, this.loadInfo.CachePct) = this.store.CacheSizeInfo;

            if (this.loadInfo.IsBusy() != null)
            {
                this.loadInfo.MarkActive();
            }

            // we suppress the load publishing if a partition is long time idle and positions are unchanged
            bool publish = false;
          
            if (this.loadInfo.CommitLogPosition < this.CommitLogPosition)
            {
                this.loadInfo.CommitLogPosition = this.CommitLogPosition;
                publish = true;
            }
            if (this.loadInfo.InputQueuePosition < this.InputQueuePosition)
            {
                this.loadInfo.InputQueuePosition = this.InputQueuePosition;
                publish = true;
            }

            if (this.loadInfo.CachePct != this.lastPublishedCachePct)
            {
                publish = true;
            }

            if (!PartitionLoadInfo.IsLongIdle(this.loadInfo.LatencyTrend))            
            {
                publish = true;
            }

            if (this.loadInfo.LatencyTrend != this.lastPublishedLatencyTrend)
            {
                publish = true;
            }

            return publish;
        }

        async ValueTask<PositionsReceived> CollectSendAndReceivePositions()
        {
            int numberPartitions = (int)this.partition.NumberPartitions();
            if (numberPartitions > 1)
            {
                var positionsReceived = new PositionsReceived()
                {
                    PartitionId = this.partition.PartitionId,
                    RequestId = Guid.NewGuid(),
                    ReceivePositions = new (long, int)?[numberPartitions],
                    NextNeededAck = new (long, int)?[numberPartitions],
                };
                ((DedupState)await this.store.ReadAsync(TrackedObjectKey.Dedup, this.effectTracker)).RecordPositions(positionsReceived);
                ((OutboxState)await this.store.ReadAsync(TrackedObjectKey.Outbox, this.effectTracker)).RecordPositions(positionsReceived);

                return positionsReceived;
            }
            else
            {
                return null;
            }
        }

        internal enum CheckpointTrigger
        {
            None,
            CommitLogBytes,
            EventCount,
            Compaction,
            Idle
        }

        bool CheckpointDue(out CheckpointTrigger trigger, out long? compactUntil)
        {
            // in a test setting, let the test decide when to checkpoint or compact
            if (this.partition.Settings.TestHooks?.CheckpointInjector != null)
            {
                return this.partition.Settings.TestHooks.CheckpointInjector.CheckpointDue((this.store as FasterKV).Log, out trigger, out compactUntil);
            }

            trigger = CheckpointTrigger.None;
            compactUntil = null;

            long inputQueuePositionLag =
                this.InputQueuePosition - Math.Max(this.lastCheckpointedInputQueuePosition, this.LogWorker.LastCommittedInputQueuePosition);

            if (this.lastCheckpointedCommitLogPosition + this.partition.Settings.MaxNumberBytesBetweenCheckpoints <= this.CommitLogPosition)
            {
                trigger = CheckpointTrigger.CommitLogBytes;
            }
            else if (this.numberEventsSinceLastCheckpoint > this.partition.Settings.MaxNumberEventsBetweenCheckpoints
                || inputQueuePositionLag > this.partition.Settings.MaxNumberEventsBetweenCheckpoints)
            {
                trigger = CheckpointTrigger.EventCount;
            }
            else if (this.loadInfo.IsBusy() == null && DateTime.UtcNow > this.timeOfNextIdleCheckpoint)
            {
                // we have reached an idle point.
                this.ScheduleNextIdleCheckpointTime();

                compactUntil = this.store.GetCompactionTarget();
                if (compactUntil.HasValue)
                {
                    trigger = CheckpointTrigger.Compaction;
                }
                else if (this.numberEventsSinceLastCheckpoint > 0 || inputQueuePositionLag > 0)
                {
                    // we checkpoint even though not much has happened
                    trigger = CheckpointTrigger.Idle;
                }
            }
             
            return trigger != CheckpointTrigger.None;
        }

        public void ProcessInParallel(PartitionEvent partitionEvent)
        {
            switch (partitionEvent)
            {
                case PartitionReadEvent readEvent:
                    // todo: actually use a parallel session worker for prefetches
                    this.Submit(partitionEvent);
                    break;

                case PartitionQueryEvent queryEvent:
                    // async queries execute on their own task and their own session
                    Task ignored = Task.Run(() => this.store.QueryAsync(queryEvent, this.effectTracker));
                    break;
            }
        }

        void ScheduleNextIdleCheckpointTime()
        {
           // to avoid all partitions taking snapshots at the same time, align to a partition-based spot between 0.5 and 1.5 of the period
            var period = this.partition.Settings.IdleCheckpointFrequencyMs * TimeSpan.TicksPerMillisecond;
            var offset = this.partition.PartitionId * period / this.partition.NumberPartitions();
            var earliest = DateTime.UtcNow.Ticks + period / 2;
            var actual = (((earliest - offset - 1) / period) + 1) * period + offset;
            this.timeOfNextIdleCheckpoint = new DateTime(actual, DateTimeKind.Utc);
        }
        
        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                bool markPartitionAsActive = false;

                // no need to wait any longer for a notification, since we are running now
                if (this.ioCompletionNotificationCancellation != null)
                {
                    this.ioCompletionNotificationCancellation.Cancel();
                    this.ioCompletionNotificationCancellation.Dispose();
                    this.ioCompletionNotificationCancellation = null;
                }

                foreach (var partitionEvent in batch)
                {
                    if (this.isShuttingDown || this.cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    // if there are IO responses ready to process, do that first
                    this.store.CompletePending();

                    // record the current time, for measuring latency in the event processing pipeline
                    partitionEvent.IssuedTimestamp = this.partition.CurrentTimeMs;

                    // now process the read or update
                    switch (partitionEvent)
                    {
                        case PartitionUpdateEvent updateEvent:
                            await this.ProcessUpdate(updateEvent);
                            break;

                        case PartitionReadEvent readEvent:
                            // async reads may either complete immediately (on cache hit) or later (on cache miss) when CompletePending() is called
                            this.store.Read(readEvent, this.effectTracker);
                            break;

                        default:
                            throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                    }
                    
                    // we advance the input queue position for all events (even non-update)
                    // this means it can move ahead of the actually persisted position, but it also means
                    // we can persist it as part of a snapshot
                    if (partitionEvent.NextInputQueuePosition > 0)
                    {
                        this.partition.Assert(partitionEvent.NextInputQueuePosition > this.InputQueuePosition, "partitionEvent.NextInputQueuePosition > this.InputQueuePosition");
                        this.InputQueuePosition = partitionEvent.NextInputQueuePosition;
                    }

                    // if we are processing events that count as activity, our latency category is at least "low"
                    markPartitionAsActive = markPartitionAsActive || partitionEvent.CountsAsPartitionActivity;
                }

                // if we are processing events that count as activity, our latency category is at least "low"
                if (markPartitionAsActive)
                {
                    this.loadInfo.MarkActive();
                }

                if (this.isShuttingDown || this.cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                this.store.AdjustCacheSize();

                // handle progression of checkpointing state machine:  none -> pendingCompaction -> pendingIndexCheckpoint ->  pendingStoreCheckpoint -> none)
                if (this.pendingStoreCheckpoint != null)
                {
                    if (this.pendingStoreCheckpoint.IsCompleted == true)
                    {
                        (this.lastCheckpointedCommitLogPosition, this.lastCheckpointedInputQueuePosition)
                           = await this.pendingStoreCheckpoint; // observe exceptions here

                        // force collection of memory used during checkpointing
                        GC.Collect();

                        // we have reached the end of the state machine transitions
                        this.pendingStoreCheckpoint = null;
                        this.pendingCheckpointTrigger = CheckpointTrigger.None;
                        this.ScheduleNextIdleCheckpointTime();
                        this.partition.Settings.TestHooks?.CheckpointInjector?.SequenceComplete((this.store as FasterKV).Log);
                    }
                }
                else if (this.pendingIndexCheckpoint != null)
                {
                    if (this.pendingIndexCheckpoint.IsCompleted == true)
                    {
                        await this.pendingIndexCheckpoint; // observe exceptions here

                        // the store checkpoint is next
                        var token = this.store.StartStoreCheckpoint(this.CommitLogPosition, this.InputQueuePosition, this.InputQueueFingerprint, null);
                        if (token.HasValue)
                        {
                            this.pendingIndexCheckpoint = null;
                            this.pendingStoreCheckpoint = this.WaitForCheckpointAsync(false, token.Value, true);
                            this.numberEventsSinceLastCheckpoint = 0;
                        }
                    }
                }
                else if (this.pendingCompaction != null)
                {
                    if (this.pendingCompaction.IsCompleted == true)
                    {
                        await this.pendingCompaction; // observe exceptions here

                        // force collection of memory used during compaction
                        GC.Collect();

                        // the index checkpoint is next
                        var token = this.store.StartIndexCheckpoint();
                        if (token.HasValue)
                        {
                            this.pendingCompaction = null;
                            this.pendingIndexCheckpoint = this.WaitForCheckpointAsync(true, token.Value, false);
                        }
                    }
                }
                else if (this.CheckpointDue(out var trigger, out long? compactUntil))
                {
                    this.pendingCheckpointTrigger = trigger;
                    this.pendingCompaction = this.RunCompactionAsync(compactUntil);
                }
                  
                // periodically publish the partition load information and the send/receive positions
                if (this.lastPublished + PublishInterval < DateTime.UtcNow)
                {
                    this.lastPublished = DateTime.UtcNow;
                    await this.PublishLoadAndPositions();
                }

                if (this.partition.NumberPartitions() > 1 && this.partition.Settings.ActivityScheduler == ActivitySchedulerOptions.Locavore)
                {
                    var activitiesState = (await this.store.ReadAsync(TrackedObjectKey.Activities, this.effectTracker)) as ActivitiesState;
                    activitiesState.CollectLoadMonitorInformation(DateTime.UtcNow);
                }

                if (this.loadInfo.IsBusy() != null)
                {
                    // the partition is not idle, so we do delay the time for the next idle checkpoint
                    this.ScheduleNextIdleCheckpointTime();
                }

                // we always call this at the end of the loop. 
                bool allRequestsCompleted = this.store.CompletePending();

                if (!allRequestsCompleted)
                {
                    this.ioCompletionNotificationCancellation = CancellationTokenSource.CreateLinkedTokenSource(this.cancellationToken);
                    var _ = this.store.ReadyToCompletePendingAsync(this.ioCompletionNotificationCancellation.Token).AsTask().ContinueWith(x => this.Notify());
                }

                // during testing, this is a good time to check invariants in the store
                this.store.CheckInvariants();
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // o.k during termination
            }
            catch (Exception exception)
            {
                this.partition.ErrorHandler.HandleError("StoreWorker.Process", "Encountered exception while working on store", exception, true, false);
            }
        }

        public async Task<(long,long)> WaitForCheckpointAsync(bool isIndexCheckpoint, Guid checkpointToken, bool removeObsoleteCheckpoints)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            var commitLogPosition = this.CommitLogPosition;
            var inputQueuePosition = this.InputQueuePosition;
            string description = $"{(isIndexCheckpoint ? "index" : "store")} checkpoint triggered by {this.pendingCheckpointTrigger}";
            this.traceHelper.FasterCheckpointStarted(checkpointToken, description, this.store.StoreStats.Get(), commitLogPosition, inputQueuePosition);

            // first do the faster checkpoint
            await this.store.CompleteCheckpointAsync();

            if (!isIndexCheckpoint)
            {
                // wait for the commit log so it is never behind the checkpoint
                await this.LogWorker.WaitForCompletionAsync();

                // finally we write the checkpoint info file
                await this.store.FinalizeCheckpointCompletedAsync(checkpointToken);

                // notify the log worker that the log can be truncated up to the commit log position
                this.LogWorker.SetLastCheckpointPosition(commitLogPosition);
            }
 
            this.traceHelper.FasterCheckpointPersisted(checkpointToken, description, commitLogPosition, inputQueuePosition, stopwatch.ElapsedMilliseconds);

            if (removeObsoleteCheckpoints)
            {
                await this.store.RemoveObsoleteCheckpoints();
            }

            this.Notify();
            return (commitLogPosition, inputQueuePosition);
        }

        public async Task<long?> RunCompactionAsync(long? target)
        {
            if (target.HasValue)
            {
                target = await this.store.RunCompactionAsync(target.Value);

                this.partition.Settings.TestHooks?.CheckpointInjector?.CompactionComplete(this.partition.ErrorHandler);
            }

            this.Notify();
            return target;
        }

        public async Task ReplayCommitLog(LogWorker logWorker)
        {
            var startPosition = this.CommitLogPosition;
            this.traceHelper.FasterProgress($"Replaying log from {startPosition}");

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.effectTracker.IsReplaying = true;
            await logWorker.ReplayCommitLog(startPosition, this);
            stopwatch.Stop();
            this.effectTracker.IsReplaying = false;

            this.traceHelper.FasterLogReplayed(this.CommitLogPosition, this.InputQueuePosition, this.numberEventsSinceLastCheckpoint, this.CommitLogPosition - startPosition, this.store.StoreStats.Get(), stopwatch.ElapsedMilliseconds);
        }

        public void RestartThingsAtEndOfRecovery(string inputQueueFingerprint)
        {

            bool queueChange = (this.InputQueueFingerprint != inputQueueFingerprint);

            if (queueChange)
            {
                this.InputQueuePosition = 0;
                this.InputQueueFingerprint = inputQueueFingerprint;

                this.traceHelper.FasterProgress($"Resetting input queue position because of new fingerprint {inputQueueFingerprint}");
            }

            var recoveryCompletedEvent = new RecoveryCompleted()
            {
                PartitionId = this.partition.PartitionId,
                RecoveredPosition = this.CommitLogPosition,
                Timestamp = DateTime.UtcNow,
                WorkerId = this.partition.Settings.WorkerId,
                ChangedFingerprint = queueChange ? inputQueueFingerprint : null,
            };

            this.traceHelper.FasterProgress("Restarting tasks");

            this.replayChecker?.PartitionStarting(this.partition, this.store, this.CommitLogPosition, this.InputQueuePosition);

            this.partition.SubmitEvent(recoveryCompletedEvent);
        }

        public async ValueTask ReplayUpdate(PartitionUpdateEvent partitionUpdateEvent)
        {
            await this.effectTracker.ProcessUpdate(partitionUpdateEvent);

            // update the input queue position if larger 
            // it can be smaller since the checkpoint can store positions advanced by non-update events
            if (partitionUpdateEvent.NextInputQueuePosition > this.InputQueuePosition)
            {
                this.InputQueuePosition = partitionUpdateEvent.NextInputQueuePosition;
            }

            // must keep track of queue fingerprint changes detected in previous recoveries
            else if (partitionUpdateEvent.ResetInputQueue)
            {
                this.InputQueuePosition = 0;
                this.InputQueueFingerprint = ((RecoveryCompleted) partitionUpdateEvent).ChangedFingerprint;
            }

            // update the commit log position
            if (partitionUpdateEvent.NextCommitLogPosition > 0)
            {
                this.partition.Assert(partitionUpdateEvent.NextCommitLogPosition > this.CommitLogPosition, "partitionUpdateEvent.NextCommitLogPosition > this.CommitLogPosition in ReplayUpdate");
                this.CommitLogPosition = partitionUpdateEvent.NextCommitLogPosition;
            }

            this.numberEventsSinceLastCheckpoint++;
        }

        async ValueTask ProcessUpdate(PartitionUpdateEvent partitionUpdateEvent)
        {
            // the transport layer should always deliver a fresh event; if it repeats itself that's a bug
            // (note that it may not be the very next in the sequence since readonly events are not persisted in the log)
            if (partitionUpdateEvent.NextInputQueuePosition > 0 && partitionUpdateEvent.NextInputQueuePosition <= this.InputQueuePosition)
            {
                this.partition.ErrorHandler.HandleError(nameof(ProcessUpdate), "Duplicate event detected", null, false, false);
                return;
            }

            await this.effectTracker.ProcessUpdate(partitionUpdateEvent);

            if (this.replayChecker != null)
            {
                await this.replayChecker.CheckUpdate(this.partition, partitionUpdateEvent, this.store);
            }

            // update the commit log position
            if (partitionUpdateEvent.NextCommitLogPosition > 0)
            {
                this.partition.Assert(partitionUpdateEvent.NextCommitLogPosition > this.CommitLogPosition, "partitionUpdateEvent.NextCommitLogPosition > this.CommitLogPosition in ProcessUpdate");
                this.CommitLogPosition = partitionUpdateEvent.NextCommitLogPosition;
            }

            this.numberEventsSinceLastCheckpoint++;
        }

        async Task PokeLoop()
        {
            while (true)
            {
                await Task.Delay(StoreWorker.PokePeriod, this.cancellationToken);

                if (this.cancellationToken.IsCancellationRequested || this.isShuttingDown)
                {
                    break;
                }

                // periodically poke so we can checkpoint, and publish load
                this.Notify();
            }
        }
    }
}
