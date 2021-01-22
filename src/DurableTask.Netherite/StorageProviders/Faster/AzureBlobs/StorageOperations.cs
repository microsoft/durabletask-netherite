// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using Microsoft.Azure.Storage;

    public partial class BlobManager
    {
            public async Task PerformWithRetriesAsync(
            SemaphoreSlim semaphore,
            bool requireLease,
            string name,
            string intent,
            string data,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int, Task<long>> operation)
        {
            try
            {
                if (semaphore != null)
                {
                    await semaphore.WaitAsync();
                }

                Stopwatch stopwatch = new Stopwatch();
                int numAttempts = 0;

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (requireLease)
                        {
                            await this.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                        }

                        this.StorageTracer?.FasterStorageProgress($"starting {name} ({intent}) target={target} numAttempts={numAttempts} {data}");
                        stopwatch.Restart();

                        long size = await operation(numAttempts).ConfigureAwait(false);

                        stopwatch.Stop();
                        this.StorageTracer?.FasterStorageProgress($"finished {name} ({intent}) target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {data} ");

                        if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                        {
                            this.TraceHelper.FasterPerfWarning($"{name} ({intent}) attempt {numAttempts} took {stopwatch.Elapsed.TotalSeconds:F1}s, which is excessive; {data}");
                        }

                        this.TraceHelper.FasterAzureStorageAccessCompleted(intent, size, name, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                        return;
                    }
                    catch (StorageException e) when (BlobUtils.IsTransientStorageError(e, this.PartitionErrorHandler.Token) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();
                        if (BlobUtils.IsTimeout(e))
                        {
                            this.TraceHelper.FasterPerfWarning($"{name} ({intent}) attempt {numAttempts} timed out after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {data}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.HandleStorageError(name, $"storage operation {name} ({intent}) attempt {numAttempts} failed transiently, retry in {nextRetryIn}s", target, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        continue;
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        this.HandleStorageError(name, $"storage operation {name} ({intent}) failed", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                }
            }
            finally
            {
                if (semaphore != null)
                {
                    semaphore.Release();
                }
            }
        }

        public void PerformWithRetries(
            bool requireLease,
            string name,
            string intent,
            string data,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int,(long,bool)> operation)
        {
            Stopwatch stopwatch = new Stopwatch();
            int numAttempts = 0;

            while (true) // retry loop
            {
                numAttempts++;
                try
                {
                    if (requireLease)
                    {
                        this.ConfirmLeaseIsGoodForAWhile();
                    }

                    this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();

                    this.StorageTracer?.FasterStorageProgress($"starting {name} ({intent}) target={target} numAttempts={numAttempts} {data}");
                    stopwatch.Restart();

                    (long size, bool completed) = operation(numAttempts);

                    if (!completed)
                    {
                        continue;
                    }

                    stopwatch.Stop();
                    this.StorageTracer?.FasterStorageProgress($"finished {name} ({intent}) target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} size={size} {data} ");

                    this.TraceHelper.FasterAzureStorageAccessCompleted(intent, size, name, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                    if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                    {
                        this.TraceHelper.FasterPerfWarning($"{name} ({intent}) attempt {numAttempts} took {stopwatch.Elapsed.TotalSeconds:F1}s, which is excessive; {data}");
                    }

                    return;
                }
                catch (StorageException e) when (numAttempts < BlobManager.MaxRetries
                    && BlobUtils.IsTransientStorageError(e, this.PartitionErrorHandler.Token))
                {
                    stopwatch.Stop();
                    if (BlobUtils.IsTimeout(e))
                    {
                        this.TraceHelper.FasterPerfWarning($"{name} ({intent}) attempt {numAttempts} timed out after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {data}");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.HandleStorageError(name, $"storage operation {name} ({intent}) attempt {numAttempts} failed transiently, retry in {nextRetryIn}s", target, e, false, true);
                        Thread.Sleep(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    this.HandleStorageError(name, $"storage operation {name} ({intent}) failed", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
            }
        }
    }
}
