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
        internal FaultInjector FaultInjector { get; set; }

        public async Task PerformWithRetriesAsync(
            SemaphoreSlim semaphore,
            bool requireLease,
            string name,
            string intent,
            string details,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            bool failIfReadonly,
            Func<int, Task<long>> operationAsync,
            Func<Task> readETagAsync = null)
        {
            if (this.readOnlyMode && failIfReadonly)
            {
                string message = $"storage operation {name} ({intent}) cannot be performed in read-only mode";
                this.StorageTracer?.FasterStorageProgress(message);
                this.HandleStorageError(name, message, target, null, isCritical, false);
                throw new OperationCanceledException(message);
            }

            try
            {
                if (semaphore != null)
                {
                    await semaphore.WaitAsync();
                }

                Stopwatch stopwatch = new Stopwatch();
                int numAttempts = 0;
                bool mustReadETagFirst = false;

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (requireLease)
                        {
                            // we can re-establish the e-tag here because we check the lease afterwards
                            if (mustReadETagFirst)
                            {
                                await readETagAsync().ConfigureAwait(false);
                                mustReadETagFirst = false;
                            }

                            Interlocked.Increment(ref this.LeaseUsers);
                            await this.ConfirmLeaseIsGoodForAWhileAsync();
                        }

                        this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();

                        this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) started attempt {numAttempts}; target={target} {details}");

                        stopwatch.Restart();

                        this.FaultInjector?.StorageAccess(this, name, intent, target);

                        long size = await operationAsync(numAttempts).ConfigureAwait(false);

                        stopwatch.Stop();
                        this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) succeeded on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {details}");

                        if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                        {
                            this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) took {stopwatch.Elapsed.TotalSeconds:F1}s on attempt {numAttempts}, which is excessive; {details}");
                        }

                        this.TraceHelper.FasterAzureStorageAccessCompleted(intent, size, name, details, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                        return;
                    }
                    catch (Exception e) when (this.PartitionErrorHandler.IsTerminated)
                    {
                        string message = $"storage operation {name} ({intent}) was canceled";
                        this.StorageTracer?.FasterStorageProgress(message);
                        throw new OperationCanceledException(message, e);
                    }
                    catch (Exception e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();

                        if (BlobUtils.IsTimeout(e))
                        {
                            this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) timed out on attempt {numAttempts} after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {details}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.HandleStorageError(name, $"storage operation {name} ({intent}) failed transiently on attempt {numAttempts}, retry in {nextRetryIn}s", target, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        continue;
                    }
                    catch (Azure.RequestFailedException ex) when (BlobUtilsV12.PreconditionFailed(ex) && readETagAsync != null)
                    {
                        this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) failed precondition on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {details}");
                        mustReadETagFirst = true;
                        continue;
                    }
                    catch (Exception exception)
                    {
                        this.HandleStorageError(name, $"storage operation {name} ({intent}) failed on attempt {numAttempts}", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                    finally
                    {
                        if (requireLease)
                        {
                            Interlocked.Decrement(ref this.LeaseUsers);
                        }
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
            string details,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            bool failIfReadonly,
            Func<int,(long,bool)> operation)
        {
            if (this.readOnlyMode && failIfReadonly)
            {
                string message = $"storage operation {name} ({intent}) cannot be performed in read-only mode";
                this.StorageTracer?.FasterStorageProgress(message);
                this.HandleStorageError(name, message, target, null, isCritical, false);
                throw new OperationCanceledException(message);
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
                        Interlocked.Increment(ref this.LeaseUsers);
                        this.ConfirmLeaseIsGoodForAWhile();
                    }

                    this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();

                    this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) started attempt {numAttempts}; target={target} {details}");
                    stopwatch.Restart();

                    this.FaultInjector?.StorageAccess(this, name, intent, target);

                    (long size, bool completed) = operation(numAttempts);

                    if (!completed)
                    {
                        continue;
                    }

                    stopwatch.Stop();
                    this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) succeeded on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} size={size} {details} ");

                    this.TraceHelper.FasterAzureStorageAccessCompleted(intent, size, name, details, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                    if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                    {
                        this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) took {stopwatch.Elapsed.TotalSeconds:F1}s on attempt {numAttempts}, which is excessive; {details}");
                    }

                    return;
                }
                catch(Exception e) when (this.PartitionErrorHandler.IsTerminated)
                {
                    string message = $"storage operation {name} ({intent}) was canceled";
                    this.StorageTracer?.FasterStorageProgress(message);
                    throw new OperationCanceledException(message, e);  
                }
                catch (Exception e) when (numAttempts < BlobManager.MaxRetries && BlobUtils.IsTransientStorageError(e))
                {
                    stopwatch.Stop();
                    if (BlobUtils.IsTimeout(e))
                    {
                        this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) timed out on attempt {numAttempts} after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {details}");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.HandleStorageError(name, $"storage operation {name} ({intent}) failed transiently on attempt {numAttempts}, retry in {nextRetryIn}s", target, e, false, true);
                        Thread.Sleep(nextRetryIn);
                    }
                    continue;
                }
                catch (Azure.RequestFailedException ex) when (BlobUtilsV12.PreconditionFailed(ex))
                {
                    // precondition failed, which indicates we are observing a rare partition race
                    this.HandleStorageError(name, $"storage operation {name} ({intent}) failed precondition on attempt {numAttempts}", target, ex, true, true);
                }
                catch (Exception exception)
                {
                    this.HandleStorageError(name, $"storage operation {name} ({intent}) failed on attempt {numAttempts}", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                finally
                {
                    if (requireLease)
                    {
                        Interlocked.Decrement(ref this.LeaseUsers);
                    }
                }
            }
        }
    }
}
