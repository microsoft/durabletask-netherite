// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;

    // This class bundles a page blob object with a queue and a counter to ensure 
    // 1) BeginCreate is not called more than once
    // 2) No writes are issued before EndCreate
    // The creator of a BlobEntry is responsible for populating the object with an underlying Page Blob. Any subsequent callers
    // either directly write to the created page blob, or queues the write so the creator can clear it after creation is complete.
    // In-progress creation is denoted by a null value on the underlying page blob
    class BlobEntry
    {
        public CloudPageBlob PageBlob { get; private set; }
        
        ConcurrentQueue<Action<CloudPageBlob>> pendingWrites;
        readonly AzureStorageDevice azureStorageDevice;
        int waitingCount;

        /// <summary>
        /// Creates a new BlobEntry to hold the given pageBlob. The pageBlob must already be created.
        /// </summary>
        /// <param name="pageBlob"></param>
        /// <param name="azureStorageDevice"></param>
        public BlobEntry(CloudPageBlob pageBlob, AzureStorageDevice azureStorageDevice)
        {
            this.PageBlob = pageBlob;
            this.azureStorageDevice = azureStorageDevice;
            if (pageBlob == null)
            {
                // Only need to allocate a queue when we potentially need to asynchronously create a blob
                this.pendingWrites = new ConcurrentQueue<Action<CloudPageBlob>>();
                this.waitingCount = 0;
            }
        }

        /// <summary>
        /// Creates a new BlobEntry, does not initialize a page blob. Use <see cref="CreateAsync(long, CloudPageBlob)"/>
        /// for actual creation.
        /// </summary>
        public BlobEntry(AzureStorageDevice azureStorageDevice) : this(null, azureStorageDevice)
        {
        }

        /// <summary>
        /// Asynchronously invoke create on the given pageBlob.
        /// </summary>
        /// <param name="size">maximum size of the blob</param>
        /// <param name="pageBlob">The page blob to create</param>
        public async Task CreateAsync(long size, CloudPageBlob pageBlob)
        {
            if (this.waitingCount != 0)
            {
                this.azureStorageDevice.BlobManager?.HandleBlobError(nameof(CreateAsync), "expect to be called on blobs that don't already exist and exactly once", pageBlob?.Name, null, false, false);
            }

            try
            {
                await BlobManager.AsynchronousStorageReadMaxConcurrency.WaitAsync();

                var stopwatch = new Stopwatch();
                int numAttempts = 0;

                while (true) // retry loop
                {
                    numAttempts++;

                    try
                    {
                        var blobRequestOptions = BlobManager.BlobRequestOptionsDefault;

                        this.azureStorageDevice.BlobManager?.StorageTracer?.FasterStorageProgress($"starting create page blob target={pageBlob.Name} size={size}");
                        stopwatch.Restart();

                        await pageBlob.CreateAsync(size,
                            accessCondition: null, options: blobRequestOptions, operationContext: null, this.azureStorageDevice.PartitionErrorHandler.Token);

                        stopwatch.Stop();
                        this.azureStorageDevice.BlobManager?.StorageTracer?.FasterStorageProgress($"finished create page blob target={pageBlob.Name} size={size} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1}");

                        if (stopwatch.ElapsedMilliseconds > 3000)
                        {
                            this.azureStorageDevice.BlobManager?.TraceHelper.FasterPerfWarning($"CloudPageBlob.CreateAsync took {stopwatch.ElapsedMilliseconds / 1000}s, which is excessive; target={pageBlob.Name} size={size}");
                        }

                        break;
                    }
                    catch (StorageException e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();
                        if (BlobUtils.IsTimeout(e))
                        {
                            this.azureStorageDevice.BlobManager?.TraceHelper.FasterPerfWarning($"CloudPageBlob.CreateAsync timed out after {stopwatch.ElapsedMilliseconds:f1}ms, retrying now; numAttempts={numAttempts} target={pageBlob.Name} size={size}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.azureStorageDevice.BlobManager?.HandleBlobError(nameof(CreateAsync), $"could not create page blob, will retry in {nextRetryIn}s, numAttempts={numAttempts}", pageBlob.Name, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        continue;
                    }
                    catch (Exception e) when (!Utils.IsFatal(e))
                    {
                        this.azureStorageDevice.BlobManager?.HandleBlobError(nameof(CreateAsync), "could not create page blob", pageBlob.Name, e, true, this.azureStorageDevice.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                }
            }
            finally
            {
                BlobManager.AsynchronousStorageReadMaxConcurrency.Release();
            }

            // At this point the blob is fully created. After this line all consequent writers will write immediately. We just
            // need to clear the queue of pending writers.
            this.PageBlob = pageBlob;

            // Take a snapshot of the current waiting count. Exactly this many actions will be cleared.
            // Swapping in -1 will inform any stragglers that we are not taking their actions and prompt them to retry (and call write directly)
            int waitingCountSnapshot = Interlocked.Exchange(ref this.waitingCount, -1);
            Action<CloudPageBlob> action;
            // Clear actions
            for (int i = 0; i < waitingCountSnapshot; i++)
            {
                // inserts into the queue may lag behind the creation thread. We have to wait until that happens.
                // This is so rare, that we are probably okay with a busy wait.
                while (!this.pendingWrites.TryDequeue(out action)) { }
                action(pageBlob);
            }

            // Mark for deallocation for the GC
            this.pendingWrites = null;
        }

        /// <summary>
        /// Attempts to enqueue an action to be invoked by the creator after creation is done. Should only be invoked when
        /// creation is in-flight. This call is allowed to fail (and return false) if concurrently the creation is complete.
        /// The caller should call the write action directly instead of queueing in this case.
        /// </summary>
        /// <param name="writeAction">The write action to perform</param>
        /// <returns>Whether the action was successfully enqueued</returns>
        public bool TryQueueAction(Action<CloudPageBlob> writeAction)
        {
            int currentCount;
            do
            {
                currentCount = this.waitingCount;

                // If current count became -1, creation is complete. New queue entries will not be processed and we must call the action ourselves.
                if (currentCount == -1) return false;

            } while (Interlocked.CompareExchange(ref this.waitingCount, currentCount + 1, currentCount) != currentCount);

            // Enqueue last. The creation thread is obliged to wait until it has processed waitingCount many actions.
            // It is extremely unlikely that we will get scheduled out here anyways.
            this.pendingWrites.Enqueue(writeAction);
            return true;
        }
    }
}
