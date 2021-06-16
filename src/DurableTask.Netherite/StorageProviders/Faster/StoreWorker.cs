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

        readonly EffectTracker effectTracker;

        bool isShuttingDown;

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
        long timeOfNextCheckpoint;

        // periodic load publishing
        PartitionLoadInfo loadInfo;
        DateTime lastPublished;
        string lastPublishedLatencyTrend;
        public static TimeSpan PublishInterval = TimeSpan.FromSeconds(8);
        public static TimeSpan PokePeriod = TimeSpan.FromSeconds(3); // allows storeworker to checkpoint and publish load even while idle


        public StoreWorker(TrackedObjectStore store, Partition partition, FasterTraceHelper traceHelper, BlobManager blobManager, CancellationToken cancellationToken) 
            : base($"{nameof(StoreWorker)}{partition.PartitionId:D2}", true, 500, cancellationToken, partition.TraceHelper)
        {
            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.store = store;
            this.partition = partition;
            this.traceHelper = traceHelper;
            this.blobManager = blobManager;
            this.random = new Random();

            this.loadInfo = PartitionLoadInfo.FirstFrame(this.partition.Settings.WorkerId);
            this.lastPublished = DateTime.MinValue;
            this.lastPublishedLatencyTrend = "";

            // construct an effect tracker that we use to apply effects to the store
            this.effectTracker = new EffectTracker(
                this.partition,
                (key, tracker) => store.ProcessEffectOnTrackedObject(key, tracker),
                (keys) => store.RemoveKeys(keys),
                () => (this.CommitLogPosition, this.InputQueuePosition)
            );
        }

        public async Task Initialize(long initialCommitLogPosition, long initialInputQueuePosition)
        {
            this.partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.InputQueuePosition = initialInputQueuePosition;
            this.CommitLogPosition = initialCommitLogPosition;
           
            this.store.InitMainSession();

            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await this.store.CreateAsync(key).ConfigureAwait(false);
                target.OnFirstInitialization();
            }

            this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
            this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
            this.numberEventsSinceLastCheckpoint = initialCommitLogPosition;
        }

        public void StartProcessing()
        {
            this.Resume();
            var pokeLoop = this.PokeLoop();
        }

        public void SetCheckpointPositionsAfterRecovery(long commitLogPosition, long inputQueuePosition)
        {
            this.CommitLogPosition = commitLogPosition;
            this.InputQueuePosition = inputQueuePosition;

            this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
            this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
            this.numberEventsSinceLastCheckpoint = 0;
            this.ScheduleNextCheckpointTime();
        }

        internal async ValueTask TakeFullCheckpointAsync(string reason)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            if (this.store.TakeFullCheckpoint(this.CommitLogPosition, this.InputQueuePosition, out var checkpointGuid))
            {
                this.traceHelper.FasterCheckpointStarted(checkpointGuid, reason, this.store.StoreStats.Get(), this.CommitLogPosition, this.InputQueuePosition);

                // do the faster full checkpoint and then finalize it
                await this.store.CompleteCheckpointAsync().ConfigureAwait(false);
                await this.store.FinalizeCheckpointCompletedAsync(checkpointGuid).ConfigureAwait(false);

                this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
                this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
                this.numberEventsSinceLastCheckpoint = 0;

                this.traceHelper.FasterCheckpointPersisted(checkpointGuid, reason, this.CommitLogPosition, this.InputQueuePosition, stopwatch.ElapsedMilliseconds);
            }
            else
            {
                this.traceHelper.FasterProgress($"Checkpoint skipped: {reason}");
            }

            this.ScheduleNextCheckpointTime();
        }

        public async Task CancelAndShutdown()
        {
            this.traceHelper.FasterProgress("Stopping StoreWorker");

            this.isShuttingDown = true;

            await this.WaitForCompletionAsync().ConfigureAwait(false);

            // wait for any in-flight checkpoints. It is unlikely but not impossible.
            if (this.pendingIndexCheckpoint != null)
            {
                await this.pendingIndexCheckpoint.ConfigureAwait(false);
            }
            if (this.pendingStoreCheckpoint != null)
            {
                await this.pendingStoreCheckpoint.ConfigureAwait(false);
            }

            this.traceHelper.FasterProgress("Stopped StoreWorker");
        }

        public Task RunPrefetchSession(IAsyncEnumerable<TrackedObjectKey> keys)
        {
            return this.store.RunPrefetchSession(keys);
        }

        async Task PublishPartitionLoad()
        {
            foreach (var k in TrackedObjectKey.GetSingletons())
            {
                (await this.store.ReadAsync(k, this.effectTracker).ConfigureAwait(false)).UpdateLoadInfo(this.loadInfo);
            }

            this.loadInfo.MissRate = this.store.StoreStats.GetMissRate();

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
            if (!PartitionLoadInfo.IsLongIdle(this.loadInfo.LatencyTrend) || this.loadInfo.LatencyTrend != this.lastPublishedLatencyTrend)
            {
                publish = true;
            }

            if (publish)
            {
                // take the current load info and put the next frame in its place
                var loadInfoToPublish = this.loadInfo;
                this.loadInfo = loadInfoToPublish.NextFrame();
                this.lastPublished = DateTime.UtcNow;
                this.lastPublishedLatencyTrend = loadInfoToPublish.LatencyTrend;

                this.partition.TraceHelper.TracePartitionLoad(loadInfoToPublish);

                // to avoid publishing not-yet committed state, publish
                // only after the current log is persisted.
                var task = this.LogWorker.WaitForCompletionAsync()
                    .ContinueWith((t) => this.partition.LoadPublisher?.Submit((this.partition.PartitionId, loadInfoToPublish)));
            }
        }

        enum CheckpointTrigger
        {
            None,
            CommitLogBytes,
            EventCount,
            TimeElapsed
        }

        bool CheckpointDue(out CheckpointTrigger trigger)
        {
            trigger = CheckpointTrigger.None;

            long inputQueuePositionLag =
                this.InputQueuePosition - Math.Max(this.lastCheckpointedInputQueuePosition, this.LogWorker.LastCommittedInputQueuePosition);

            if (this.lastCheckpointedCommitLogPosition + this.partition.Settings.MaxNumberBytesBetweenCheckpoints <= this.CommitLogPosition)
            {
                trigger = CheckpointTrigger.CommitLogBytes;
            }
            else if (this.numberEventsSinceLastCheckpoint > this.partition.Settings.MaxNumberEventsBetweenCheckpoints
                ||   inputQueuePositionLag > this.partition.Settings.MaxNumberEventsBetweenCheckpoints)
            {
                trigger = CheckpointTrigger.EventCount;
            }
            else if (
                (this.numberEventsSinceLastCheckpoint > 0 || inputQueuePositionLag > 0)
                && DateTime.UtcNow.Ticks > this.timeOfNextCheckpoint)
            {
                trigger = CheckpointTrigger.TimeElapsed;
            }
             
            return trigger != CheckpointTrigger.None;
        }

        void ScheduleNextCheckpointTime()
        {
            // to avoid all partitions taking snapshots at the same time, align to a partition-based spot
            var period = this.partition.Settings.MaxTimeMsBetweenCheckpoints * TimeSpan.TicksPerMillisecond;
            var offset = (this.partition.PartitionId * period / this.partition.NumberPartitions());
            var maxTime = DateTime.UtcNow.Ticks + period;
            this.timeOfNextCheckpoint = (((maxTime - offset) / period) * period) + offset;
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
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
                            await this.ProcessUpdate(updateEvent).ConfigureAwait(false);
                            break;

                        case PartitionReadEvent readEvent:
                            readEvent.OnReadIssued(this.partition);
                            // async reads may either complete immediately (on cache hit) or later (on cache miss) when CompletePending() is called
                            this.store.ReadAsync(readEvent, this.effectTracker);
                            break;

                        case PartitionQueryEvent queryEvent:
                            // async queries execute on their own task and their own session
                            Task ignored = Task.Run(() => this.store.QueryAsync(queryEvent, this.effectTracker));
                            break;

                        default:
                            throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                    }
                    
                    // we advance the input queue position for all events (even non-update)
                    // this means it can move ahead of the actually persisted position, but it also means
                    // we can persist it as part of a snapshot
                    if (partitionEvent.NextInputQueuePosition > 0)
                    {
                        this.partition.Assert(partitionEvent.NextInputQueuePosition > this.InputQueuePosition);
                        this.InputQueuePosition = partitionEvent.NextInputQueuePosition;
                    }

                    // since we are processing actual events, our latency category is at least "low"
                    this.loadInfo.MarkActive();
                }

                if (this.isShuttingDown || this.cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                // handle progression of checkpointing state machine (none -> index pending -> store pending -> none)
                if (this.pendingStoreCheckpoint != null)
                {
                    if (this.pendingStoreCheckpoint.IsCompleted == true)
                    {
                        (this.lastCheckpointedCommitLogPosition, this.lastCheckpointedInputQueuePosition)
                            = await this.pendingStoreCheckpoint.ConfigureAwait(false); // observe exceptions here
                        this.pendingStoreCheckpoint = null;
                        this.pendingCheckpointTrigger = CheckpointTrigger.None;
                        this.ScheduleNextCheckpointTime();
                    }
                }
                else if (this.pendingIndexCheckpoint != null)
                {
                    if (this.pendingIndexCheckpoint.IsCompleted == true)
                    {
                        await this.pendingIndexCheckpoint.ConfigureAwait(false); // observe exceptions here
                        this.pendingIndexCheckpoint = null;
                        var token = this.store.StartStoreCheckpoint(this.CommitLogPosition, this.InputQueuePosition);
                        if (token.HasValue)
                        {
                            this.pendingStoreCheckpoint = this.WaitForCheckpointAsync(false, token.Value);
                            this.numberEventsSinceLastCheckpoint = 0;
                        }
                    }
                }
                else if (this.CheckpointDue(out var trigger))
                {
                    var token = this.store.StartIndexCheckpoint();
                    if (token.HasValue)
                    {
                        this.pendingCheckpointTrigger = trigger;
                        this.pendingIndexCheckpoint = this.WaitForCheckpointAsync(true, token.Value);
                    }
                }
                
                if (this.lastPublished + PublishInterval < DateTime.UtcNow)
                {
                    await this.PublishPartitionLoad().ConfigureAwait(false);
                }

                var activitiesState = (await this.store.ReadAsync(TrackedObjectKey.Activities, this.effectTracker).ConfigureAwait(false)) as ActivitiesState;
                activitiesState.CollectLoadMonitorInformation();

                if (this.lastCheckpointedCommitLogPosition == this.CommitLogPosition 
                    && this.lastCheckpointedInputQueuePosition == this.InputQueuePosition
                    && this.LogWorker.LastCommittedInputQueuePosition <= this.InputQueuePosition)
                {
                    // since there were no changes since the last snapshot 
                    // we can pretend that it was taken just now
                    this.ScheduleNextCheckpointTime();
                }

                // make sure to complete ready read requests, or notify this worker
                // if any read requests become ready to process at some point
                var t = this.store.ReadyToCompletePendingAsync();
                if (!t.IsCompleted)
                {
                    var ignoredTask = t.AsTask().ContinueWith(x => this.Notify());
                }

                // we always call this at the end of the loop. 
                this.store.CompletePending();
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

        public async Task<(long,long)> WaitForCheckpointAsync(bool isIndexCheckpoint, Guid checkpointToken)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            var commitLogPosition = this.CommitLogPosition;
            var inputQueuePosition = this.InputQueuePosition;
            string description = $"{(isIndexCheckpoint ? "index" : "store")} checkpoint triggered by {this.pendingCheckpointTrigger}";
            this.traceHelper.FasterCheckpointStarted(checkpointToken, description, this.store.StoreStats.Get(), commitLogPosition, inputQueuePosition);

            // first do the faster checkpoint
            await this.store.CompleteCheckpointAsync().ConfigureAwait(false);

            if (!isIndexCheckpoint)
            {
                // wait for the commit log so it is never behind the checkpoint
                await this.LogWorker.WaitForCompletionAsync().ConfigureAwait(false);

                // finally we write the checkpoint info file
                await this.store.FinalizeCheckpointCompletedAsync(checkpointToken).ConfigureAwait(false);

                // notify the log worker that the log can be truncated up to the commit log position
                this.LogWorker.SetLastCheckpointPosition(commitLogPosition);
            }
 
            this.traceHelper.FasterCheckpointPersisted(checkpointToken, description, commitLogPosition, inputQueuePosition, stopwatch.ElapsedMilliseconds);

            this.Notify();
            return (commitLogPosition, inputQueuePosition);
        }

        public async Task ReplayCommitLog(LogWorker logWorker)
        {
            var startPosition = this.CommitLogPosition;
            this.traceHelper.FasterProgress($"Replaying log from {startPosition}");

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.effectTracker.IsReplaying = true;
            await logWorker.ReplayCommitLog(startPosition, this).ConfigureAwait(false);
            stopwatch.Stop();
            this.effectTracker.IsReplaying = false;

            this.traceHelper.FasterLogReplayed(this.CommitLogPosition, this.InputQueuePosition, this.numberEventsSinceLastCheckpoint, this.CommitLogPosition - startPosition, this.store.StoreStats.Get(), stopwatch.ElapsedMilliseconds);
        }

        public async Task RestartThingsAtEndOfRecovery()
        {
            this.traceHelper.FasterProgress("Restarting tasks");

            using (EventTraceContext.MakeContext(this.CommitLogPosition, string.Empty))
            {
                foreach (var key in TrackedObjectKey.GetSingletons())
                {
                    var target = (TrackedObject)await this.store.ReadAsync(key, this.effectTracker).ConfigureAwait(false);
                    target.OnRecoveryCompleted();
                }
            }
        }

        public async ValueTask ReplayUpdate(PartitionUpdateEvent partitionUpdateEvent)
        {
            await this.effectTracker.ProcessUpdate(partitionUpdateEvent).ConfigureAwait(false);

            // update the input queue position if larger 
            // it can be smaller since the checkpoint can store positions advanced by non-update events
            if (partitionUpdateEvent.NextInputQueuePosition > this.InputQueuePosition)
            {
                this.InputQueuePosition = partitionUpdateEvent.NextInputQueuePosition;
            }

            // update the commit log position
            if (partitionUpdateEvent.NextCommitLogPosition > 0)
            {
                this.partition.Assert(partitionUpdateEvent.NextCommitLogPosition > this.CommitLogPosition);
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

            await this.effectTracker.ProcessUpdate(partitionUpdateEvent).ConfigureAwait(false);

            // update the commit log position
            if (partitionUpdateEvent.NextCommitLogPosition > 0)
            {
                this.partition.Assert(partitionUpdateEvent.NextCommitLogPosition > this.CommitLogPosition);
                this.CommitLogPosition = partitionUpdateEvent.NextCommitLogPosition;
            }

            this.numberEventsSinceLastCheckpoint++;
        }

        async Task PokeLoop()
        {
            while (true)
            {
                await Task.Delay(StoreWorker.PokePeriod, this.cancellationToken).ConfigureAwait(false);

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
