// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Superclass for the store component which manages the in-memory tracked objects and their storage checkpoints.
    /// </summary>
    abstract class TrackedObjectStore
    {
        public abstract void InitMainSession();

        public abstract Task<bool> FindCheckpointAsync(bool logIsEmpty);

        public abstract Task<(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint)> RecoverAsync();

        public abstract bool CompletePending();

        public abstract ValueTask ReadyToCompletePendingAsync(CancellationToken token);

        public abstract void AdjustCacheSize();

        public abstract bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint, out Guid checkpointGuid);

        public abstract Task RemoveObsoleteCheckpoints();

        public abstract Guid? StartIndexCheckpoint();

        public abstract Guid? StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint, long? shiftBeginAddress);

        public abstract ValueTask CompleteCheckpointAsync();

        public abstract Task FinalizeCheckpointCompletedAsync(Guid guid);

        public abstract long? GetCompactionTarget();

        public abstract Task<long> RunCompactionAsync(long target);

        public abstract void CheckInvariants();

        // perform a query
        public abstract Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker);

        // run a prefetch thread
        public abstract Task RunPrefetchSession(IAsyncEnumerable<TrackedObjectKey> keys);

        // kick off a read of a tracked object, completing asynchronously if necessary
        public abstract void Read(PartitionReadEvent readEvent, EffectTracker effectTracker);

        // read a singleton tracked object on the main session and wait for the response (only one of these is executing at a time)
        public abstract ValueTask<TrackedObject> ReadAsync(FasterKV.Key key, EffectTracker effectTracker);

        // create a tracked object on the main session (only one of these is executing at a time)
        public abstract ValueTask<TrackedObject> CreateAsync(FasterKV.Key key);

        public abstract ValueTask ProcessEffectOnTrackedObject(FasterKV.Key k, EffectTracker tracker);

        public abstract ValueTask RemoveKeys(IEnumerable<TrackedObjectKey> keys);

        public abstract void EmitCurrentState(Action<TrackedObjectKey, TrackedObject> emitItem);     

        public StoreStatistics StoreStats { get; } = new StoreStatistics();

        public abstract (double totalSizeMB, int fillPercentage) CacheSizeInfo { get; }

        public class StoreStatistics
        {
            public long Create;
            public long Modify;
            public long Read;
            public long Copy;
            public long Serialize;
            public long Deserialize;

            public string Get()
            {
                var result = $"(Cr={this.Create} Mod={this.Modify} Rd={this.Read} Cpy={this.Copy} Ser={this.Serialize} Des={this.Deserialize})";

                this.Create = 0;
                this.Modify = 0;
                this.Read = 0;
                this.Copy = 0;
                this.Serialize = 0;
                this.Deserialize = 0;

                return result;
            }

            public long HitCount;
            public long MissCount;

            public double GetMissRate()
            {
                double ratio = (this.MissCount > 0) ? ((double)this.MissCount / (this.MissCount + this.HitCount)) : 0.0;
                this.HitCount = this.MissCount = 0;
                return ratio;
            }
        }
    }
}
