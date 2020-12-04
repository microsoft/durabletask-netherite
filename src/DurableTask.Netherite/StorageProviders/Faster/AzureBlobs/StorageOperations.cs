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
            string description,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int, Task> operation)
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

                        this.StorageTracer?.FasterStorageProgress($"starting {name} target={target} numAttempts={numAttempts} {description}");
                        stopwatch.Restart();

                        await operation(numAttempts).ConfigureAwait(false);

                        stopwatch.Stop();
                        this.StorageTracer?.FasterStorageProgress($"finished {name} target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {description} ");

                        if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                        {
                            this.TraceHelper.FasterPerfWarning($"{name} took {stopwatch.Elapsed.TotalSeconds:F1}s, which is excessive; {description}");
                        }

                        return;
                    }
                    catch (StorageException e) when (BlobUtils.IsTransientStorageError(e, this.PartitionErrorHandler.Token) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();
                        if (BlobUtils.IsTimeout(e))
                        {
                            this.TraceHelper.FasterPerfWarning($"{name} timed out after {stopwatch.ElapsedMilliseconds:F1}ms, retrying now; target={target} {description}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.HandleStorageError(name, $"storage operation {name} attempt {numAttempts} failed, retry in {nextRetryIn}s", target, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        continue;
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        this.HandleStorageError(name, $"storage operation {name} failed", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
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
            string description,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int,bool> operation)
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

                    this.StorageTracer?.FasterStorageProgress($"starting {name} target={target} numAttempts={numAttempts} {description}");
                    stopwatch.Restart();

                    bool completed = operation(numAttempts);

                    if (!completed)
                    {
                        continue;
                    }

                    stopwatch.Stop();
                    this.StorageTracer?.FasterStorageProgress($"finished {name} target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {description} ");

                    if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                    {
                        this.TraceHelper.FasterPerfWarning($"{name} took {stopwatch.Elapsed.TotalSeconds:F1}s, which is excessive; {description}");
                    }

                    return;
                }
                catch (StorageException e) when (numAttempts < BlobManager.MaxRetries
                    && BlobUtils.IsTransientStorageError(e, this.PartitionErrorHandler.Token))
                {
                    stopwatch.Stop();
                    if (BlobUtils.IsTimeout(e))
                    {
                        this.TraceHelper.FasterPerfWarning($"{name} timed out after {stopwatch.ElapsedMilliseconds:F1}ms, retrying now; target={target} {description}");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.HandleStorageError(name, $"storage operation {name} attempt {numAttempts} failed, retry in {nextRetryIn}s", target, e, false, true);
                        Thread.Sleep(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    this.HandleStorageError(name, $"storage operation {name} failed", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
            }
        }
    }
}
