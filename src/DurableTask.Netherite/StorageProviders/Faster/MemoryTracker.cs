// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using FASTER.core;

    /// <summary>
    /// Records cache and storage management traces for each object. This class is only used for testing and debugging, as it creates lots of overhead.
    /// </summary>
    class MemoryTracker
    {
        readonly FasterStorage fasterStorage;
        readonly ConcurrentDictionary<CacheTracker, CacheTracker> stores;

        public MemoryTracker(FasterStorage fasterStorage)
        {
            this.fasterStorage = fasterStorage;
            this.stores = new ConcurrentDictionary<CacheTracker, CacheTracker>();
        }

        public CacheTracker NewCacheTracker(FasterKV store)
        {
            var cacheTracker = new CacheTracker(this, store);
            this.stores.TryAdd(cacheTracker, cacheTracker);
            this.UpdateTargetSizes();
            return cacheTracker;
        }

        public void UpdateTargetSizes()
        {
            if (this.stores.Count > 0)
            {
                long targetSize = this.fasterStorage.TargetMemorySize / this.stores.Count;
                foreach (var s in this.stores.Keys)
                {
                    s.SetTargetSize(targetSize);
                }
            }
        }

        public class CacheTracker : IDisposable
        {
            readonly MemoryTracker memoryTracker;
            readonly FasterKV store;

            long trackedObjectSize;

            public long TrackedObjectSize => Interlocked.Read(ref this.trackedObjectSize);

            public void UpdateTrackedObjectSize(long delta)
            {
                long trackedObjectSize = Interlocked.Add(ref this.trackedObjectSize, delta);
                //this.store.AdjustPageCount(this.TargetSize, trackedObjectSize);
            }

            public long TargetSize { get; set; }

            public CacheTracker(MemoryTracker memoryTracker, FasterKV store)
            {
                this.memoryTracker = memoryTracker;
                this.store = store;
            }

            public void Dispose()
            {
                if (this.memoryTracker.stores.TryRemove(this, out _))
                {
                    this.memoryTracker.UpdateTargetSizes();
                }
            }

            public void SetTargetSize(long newTargetSize)
            {
                this.TargetSize = newTargetSize;
                //this.store.AdjustPageCount(this.TargetSize, this.TrackedObjectSize);
            }
        }
    }
}
