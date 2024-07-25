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
    using Azure.Storage.Blobs.Specialized;
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
            PermanentFail,
        }

        InjectionMode mode;
        bool injectDuringStartup;
        bool injectLeaseRenewals;
        int countdown;
        int nextrun;

        public int RandomProbability { get; set; }
        readonly Random random = new Random();

        int failedRestarts = 1;

        public void StartNewTest()
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: StartNewTest");
        }

        public IDisposable WithMode(InjectionMode mode, bool injectDuringStartup = false, bool injectLeaseRenewals = false)
        {
            System.Diagnostics.Trace.TraceInformation($"FaultInjector: SetMode {mode}");

            this.mode = mode;
            this.injectDuringStartup = injectDuringStartup;
            this.injectLeaseRenewals = injectLeaseRenewals;

            switch (mode)
            {
                case InjectionMode.IncrementSuccessRuns:
                    this.countdown = 0;
                    this.nextrun = 1;
                    break;
                case InjectionMode.PermanentFail:
                    this.countdown = -1;
                    this.nextrun = -1;
                    break;

                default:
                    break;
            }

            return new ResetMode() { FaultInjector = this };
        }

        class ResetMode : IDisposable
        {
            public FaultInjector FaultInjector { get; set; }

            public void Dispose()
            {
                System.Diagnostics.Trace.TraceInformation($"FaultInjector: Reset");

                this.FaultInjector.mode = InjectionMode.None;
                this.FaultInjector.injectDuringStartup = false;
            }
        }

        readonly Dictionary<int, TaskCompletionSource<object>> startupWaiters = new Dictionary<int, TaskCompletionSource<object>>();
        readonly HashSet<BlobManager> startedPartitions = new HashSet<BlobManager>();

        public void StartClient()
        {
            if (this.mode == InjectionMode.PermanentFail)
            {
                throw new Exception("Injected failure when staring client!");
            }
        }


        public async Task WaitForStartup(int numPartitions, TimeSpan timeout)
        {
            var tasks = new Task[numPartitions];
            for (int i = 0; i < numPartitions; i++)
            {
                var tcs = new TaskCompletionSource<object>();
                this.startupWaiters.Add(i, tcs);
                tasks[i] = tcs.Task;
            }
            var timeoutTask = Task.Delay(timeout);
            var allDone = Task.WhenAll(tasks);
            await Task.WhenAny(timeoutTask, allDone);
            if (!allDone.IsCompleted)
            {
                throw new TimeoutException($"FaultInjector.WaitForStartup timed out after {timeout}");
            }
            await allDone;
        }

        public async Task BreakLease(Microsoft.Azure.Storage.Blob.CloudBlockBlob blob)
        {
            await blob.BreakLeaseAsync(TimeSpan.Zero);
        }

        internal async Task BreakLease(BlobUtilsV12.BlockBlobClients blob)
        {
            await blob.Default.GetBlobLeaseClient().BreakAsync(TimeSpan.Zero);
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

            if (this.injectLeaseRenewals || (intent != "RenewLease"))
            {
                if (this.injectDuringStartup || this.startedPartitions.Contains(blobManager))
                {
                    if (this.mode == InjectionMode.IncrementSuccessRuns)
                    {
                        if (this.countdown-- <= 0)
                        {
                            pass = false;
                            this.countdown = this.nextrun++;
                        }
                    }
                    else if (this.mode == InjectionMode.PermanentFail)
                    {
                        pass = false;
                    }
                }
            }

            if (this.RandomProbability > 0)
            {
                if (this.failedRestarts > 0 && this.startedPartitions.Contains(blobManager))
                {
                    this.failedRestarts = 0;
                }

                if (this.failedRestarts < 2)
                {
                    var dieRoll = this.random.Next(this.RandomProbability * (1 + this.failedRestarts));

                    if (dieRoll == 0)
                    {
                        pass = false;
                        this.failedRestarts++;
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
