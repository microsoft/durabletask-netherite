// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using FASTER.core;

    /// <summary>
    /// Track memory use by all FASTER caches.
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

        public CacheTracker NewCacheTracker(FasterKV store, CacheDebugger cacheDebugger)
        {
            var cacheTracker = new CacheTracker(this, store, cacheDebugger);
            this.stores.TryAdd(cacheTracker, cacheTracker);
            this.UpdateTargetSizes();
            return cacheTracker;
        }

        public void UpdateTargetSizes()
        {
            if (this.stores.Count > 0)
            {
                long targetSize = this.TotalAvailableMemory / this.stores.Count;
                foreach (var s in this.stores.Keys)
                {
                    s.SetTargetSize(targetSize);
                }
            }
        }

        long TotalAvailableMemory => this.fasterStorage.TargetMemorySize;

        public (int, long) GetMemorySize()
        {
            (int totalPages, long totalSize) = (0, 0);
            foreach(var store in this.stores.Values)
            {
                (int numPages, long size) = store.ComputeMemorySize();
                totalPages += numPages;
                totalSize += size;
            }
            return (totalPages, totalSize);
        }

        public void DecrementPages()
        {
            foreach (var store in this.stores.Values)
            {
                store.DecrementPages();
            }
        }

        public class CacheTracker : IDisposable
        {
            readonly MemoryTracker memoryTracker;
            readonly FasterKV store;
            readonly CacheDebugger cacheDebugger;

            long trackedObjectSize;

            public LogAccessor<FasterKV.Key, FasterKV.Value> Log { private get; set; }

            public long TrackedObjectSize => Interlocked.Read(ref this.trackedObjectSize);

            public long TargetSize { get; set; }

            public long TotalAvailableMemory => this.memoryTracker.TotalAvailableMemory;

            public CacheTracker(MemoryTracker memoryTracker, FasterKV store, CacheDebugger cacheDebugger)
            {
                this.memoryTracker = memoryTracker;
                this.store = store;
                this.cacheDebugger = cacheDebugger;
            }

            public void Dispose()
            {
                if (this.memoryTracker.stores.TryRemove(this, out _))
                {
                    this.memoryTracker.UpdateTargetSizes();
                }
            }

            public void MeasureCacheSize()
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                (int numPages, long size) = this.store.ComputeMemorySize();
                double MB(long bytes) => (double)bytes / (1024 * 1024);
                this.store.TraceHelper.FasterProgress($"CacheSize: numPages={numPages} objectSize={MB(size):F2}MB totalSize={MB(size + this.store.MemoryUsedWithoutObjects):F2}MB elapsedMs={stopwatch.Elapsed.TotalMilliseconds:F2}");
                this.trackedObjectSize = size;
            }

            public (int, long) ComputeMemorySize() => this.store.ComputeMemorySize();

            public void DecrementPages() => this.store.DecrementPages();

            public void SetTargetSize(long newTargetSize)
            {
                this.TargetSize = newTargetSize;
                this.store.AdjustPageCount(this.TargetSize, this.TrackedObjectSize);
            }

            public void OnEviction(long totalSize)
            {
                Interlocked.Add(ref this.trackedObjectSize, -totalSize);
                this.store.AdjustPageCount(this.TargetSize, this.TrackedObjectSize);
            }

            internal void UpdateTrackedObjectSize(long delta, TrackedObjectKey key, long? address)
            {
                Interlocked.Add(ref this.trackedObjectSize, delta);
                this.cacheDebugger?.UpdateTrackedObjectSize(delta, key, address);
            }
        }
    }
}
