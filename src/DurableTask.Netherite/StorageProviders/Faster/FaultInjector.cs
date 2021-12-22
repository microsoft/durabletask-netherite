// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using FASTER.core;
    using Microsoft.Azure.Storage;

    /// <summary>
    /// Injects faults into storage accesses.
    /// </summary>
    public class FaultInjector
    {
        public enum InjectionMode
        {
            None,
            IncrementSuccessRuns,
        }

        InjectionMode mode;
        int countdown;
        int nextrun;

        public void StartNewTest()
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: StartNewTest");
        }

        public void SetMode(InjectionMode mode)
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: SetMode {mode}");

            this.mode = mode;

            switch (mode)
            {
                case InjectionMode.IncrementSuccessRuns:
                    this.countdown = 0;
                    this.nextrun = 1;
                    break;

                default:
                    break;
            }
        }

        readonly Dictionary<int, TaskCompletionSource<object>> startupWaiters = new Dictionary<int, TaskCompletionSource<object>>();
        readonly HashSet<string> excludedIntent = new HashSet<string>();
        readonly HashSet<BlobManager> startedPartitions = new HashSet<BlobManager>();

        public bool InjectOnStartup { get; set; }

        public Task WaitForStartup(int numPartitions)
        {
            var tasks = new Task[numPartitions];
            for (int i = 0; i < numPartitions; i++)
            {
                var tcs = new TaskCompletionSource<object>();
                this.startupWaiters.Add(i, tcs);
                tasks[i] = tcs.Task;
            }
            return Task.WhenAll(tasks);
        }

        public async Task BreakLease(Microsoft.Azure.Storage.Blob.CloudBlockBlob blob)
        {
            await blob.BreakLeaseAsync(TimeSpan.Zero);
        }

        public void Exclude(string intent)
        {
            this.excludedIntent.Add(intent);
        }

        public void Starting(BlobManager blobManager)
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: P{blobManager.PartitionId:D2} Starting");
        }

        public void Started(BlobManager blobManager)
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: P{blobManager.PartitionId:D2} Started");
            this.startedPartitions.Add(blobManager);

            if (this.startupWaiters.TryGetValue(blobManager.PartitionId, out var tcs))
            {
                tcs.TrySetResult(null);
            }
        }

        public void Disposed(BlobManager blobManager)
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: P{blobManager.PartitionId:D2} Disposed");
            this.startedPartitions.Remove(blobManager);
        }


        public void StorageAccess(BlobManager blobManager, string name, string intent, string target)
        {
            bool pass = true;

            if (!this.excludedIntent.Contains(intent) 
                && (this.InjectOnStartup || this.startedPartitions.Contains(blobManager)))
            {
                if (this.mode == InjectionMode.IncrementSuccessRuns)
                {
                    if (this.countdown-- <= 0)
                    {
                        pass = false;
                        this.countdown = this.nextrun++;
                    }
                }
            }

            System.Diagnostics.Trace.TraceInformation($"FaultInjector: P{blobManager.PartitionId:D2} {(pass ? "PASS" : "FAIL")} StorageAccess {name} {intent} {target}");

            if (!pass)
            {
                this.startedPartitions.Remove(blobManager);
                throw new Exception("Injected Fault");
            }
        }
    }
}
