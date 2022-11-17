// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;


    class TrackedObjectStoreEffectTracker : PartitionEffectTracker
    {
        readonly TrackedObjectStore store;
        readonly StoreWorker storeWorker;

        public TrackedObjectStoreEffectTracker(Partition partition, StoreWorker storeWorker, TrackedObjectStore store)
            : base(partition)
        {
            this.store = store;
            this.storeWorker = storeWorker;
        }

        public override ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
        {
            return this.store.ProcessEffectOnTrackedObject(key, tracker);
        }

        public override (long, long) GetPositions()
        {
            return (this.storeWorker.CommitLogPosition, this.storeWorker.InputQueuePosition);
        }

        public override ValueTask RemoveFromStore(IEnumerable<TrackedObjectKey> keys)
        {
            this.store.RemoveKeys(keys);
            return default;
        }
    }
}