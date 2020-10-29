//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Superclass for the store component which manages the in-memory tracked objects and their storage checkpoints.
    /// </summary>
    abstract class TrackedObjectStore
    {
        public abstract void InitMainSession();

        public abstract void Recover(out long commitLogPosition, out long inputQueuePosition);

        public abstract void CompletePending();

        public abstract ValueTask ReadyToCompletePendingAsync();

        public abstract bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, out Guid checkpointGuid);

        public abstract Guid StartIndexCheckpoint();

        public abstract Guid StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition);

        public abstract ValueTask CompleteCheckpointAsync();

        public abstract Task FinalizeCheckpointCompletedAsync(Guid guid);

        // perform a query
        public abstract Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker);

        // kick off a read of a tracked object, completing asynchronously if necessary
        public abstract void ReadAsync(PartitionReadEvent readEvent, EffectTracker effectTracker);

        // read a tracked object on the main session and wait for the response (only one of these is executing at a time)
        public abstract ValueTask<TrackedObject> ReadAsync(FasterKV.Key key, EffectTracker effectTracker);

        // create a tracked object on the main session (only one of these is executing at a time)
        public abstract ValueTask<TrackedObject> CreateAsync(FasterKV.Key key);

        public abstract ValueTask ProcessEffectOnTrackedObject(FasterKV.Key k, EffectTracker tracker);

        public StoreStatistics StoreStats { get; } = new StoreStatistics();

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
