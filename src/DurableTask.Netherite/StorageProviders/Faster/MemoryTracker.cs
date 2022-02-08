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
    using System.Threading.Tasks;
    using FASTER.core;

    /// <summary>
    /// Track memory use by all FASTER caches.
    /// </summary>
    class MemoryTracker
    {
        readonly long maxCacheSize;
        readonly ConcurrentDictionary<CacheTracker, CacheTracker> stores;

        public long MaxCacheSize => this.maxCacheSize;

        public MemoryTracker(long maxCacheSize)
        {
            this.maxCacheSize = maxCacheSize;
            this.stores = new ConcurrentDictionary<CacheTracker, CacheTracker>();
        }

        public CacheTracker NewCacheTracker(FasterKV store, int partitionId, CacheDebugger cacheDebugger)
        {
            var cacheTracker = new CacheTracker(this, partitionId, store, cacheDebugger);
            this.stores.TryAdd(cacheTracker, cacheTracker);
            this.UpdateTargetSizes();
            return cacheTracker;
        }

        public void UpdateTargetSizes()
        {
            if (this.stores.Count > 0)
            {
                long targetSize = this.maxCacheSize / this.stores.Count;
                foreach (var s in this.stores.Keys)
                {
                    s.SetTargetSize(targetSize);
                }
            }
        }

        internal (int, long) GetMemorySize() // used for testing only
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

        internal void SetEmptyPageCount(int emptyPageCount) // used by tests only
        {
            foreach (var store in this.stores.Values)
            {
                store.SetEmptyPageCount(emptyPageCount);
            }
        }

        public class CacheTracker : BatchWorker<object>, IDisposable
        {
            readonly MemoryTracker memoryTracker;
            readonly FasterKV store;
            readonly CacheDebugger cacheDebugger;
            readonly int pageSize;

            long trackedObjectSize;

            public long TrackedObjectSize => Interlocked.Read(ref this.trackedObjectSize);

            public long TargetSize { get; set; }

            public long MaxCacheSize => this.memoryTracker.maxCacheSize;

            public CacheTracker(MemoryTracker memoryTracker, int partitionId, FasterKV store, CacheDebugger cacheDebugger)
                : base($"CacheTracker{partitionId:D2}", false, 10000, CancellationToken.None, null)
            {
                this.memoryTracker = memoryTracker;
                this.store = store;
                this.cacheDebugger = cacheDebugger;
                this.pageSize = store.PageSize;
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
                (int numPages, long size) = this.store.ComputeMemorySize(true);
                double MB(long bytes) => (double)bytes / (1024 * 1024);
                this.store.TraceHelper.FasterProgress($"CacheSize: numPages={numPages} objectSize={MB(size):F2}MB totalSize={MB(size + this.store.MemoryUsedWithoutObjects):F2}MB elapsedMs={stopwatch.Elapsed.TotalMilliseconds:F2}");
                this.trackedObjectSize = size;
            }

            public (int, long) ComputeMemorySize() => this.store.ComputeMemorySize(false); // used by tests only

            internal void SetEmptyPageCount(int emptyPageCount) => this.store.SetEmptyPageCount(emptyPageCount); // used by tests only

            public void SetTargetSize(long newTargetSize)
            {
                this.TargetSize = newTargetSize;
                this.Notify();
            }

            public void OnEviction(long totalSize, long endAddress)
            {
                Interlocked.Add(ref this.trackedObjectSize, -totalSize);
                this.Notify();
            }

            internal void UpdateTrackedObjectSize(long delta, TrackedObjectKey key, long? address)
            {
                Interlocked.Add(ref this.trackedObjectSize, delta);
                this.cacheDebugger?.UpdateTrackedObjectSize(delta, key, address);
            }

            protected override Task Process(IList<object> _)
            {
                var log = this.store.Log;

                if (log != null)
                {
                    long excess = Interlocked.Read(ref this.trackedObjectSize) + this.store.MemoryUsedWithoutObjects - this.TargetSize;
                    int actuallyEmptyPages = (int)((log.BufferSize - ((log.TailAddress - log.HeadAddress) / this.pageSize)));
                    int tighten = Math.Min(actuallyEmptyPages + 1, log.BufferSize - 2);
                    int loosen = 0;

                    if (excess > 50000 && log.EmptyPageCount < tighten)
                    {
                        this.store.TraceHelper.FasterStorageProgress($"tighten memory control: set empty pages to {tighten}");
                        log.SetEmptyPageCount(tighten, true);
                        this.Notify();
                    }
                    else if (excess < -50000 && log.EmptyPageCount > loosen)
                    {
                        this.store.TraceHelper.FasterStorageProgress($"loosen memory control: set empty pages to {loosen}");
                        log.SetEmptyPageCount(loosen, true);
                        this.Notify();
                    }
                }

                return Task.CompletedTask;
            }
        }
    }
}
