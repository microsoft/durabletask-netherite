// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using FASTER.core;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;

    /// <summary>
    /// A IDevice Implementation that is backed by<see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
    /// This device is slower than a local SSD or HDD, but provides scalability and shared access in the cloud.
    /// </summary>
    class AzureStorageDevice : StorageDeviceBase
    {
        readonly ConcurrentDictionary<int, BlobEntry> blobs;
        readonly CloudBlobDirectory blockBlobDirectory;
        readonly CloudBlobDirectory pageBlobDirectory;
        readonly string blobName;
        readonly bool underLease;

        internal IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        // Azure Page Blobs have a fixed sector size of 512 bytes.
        const uint PAGE_BLOB_SECTOR_SIZE = 512;
        // Max upload size must be at most 4MB
        // we use an even smaller value to improve retry/timeout behavior in highly contended situations
        // Also, this allows us to use aggressive timeouts to kill stragglers
        const uint MAX_UPLOAD_SIZE = 1024 * 1024;

        const long MAX_PAGEBLOB_SIZE = 512L * 1024 * 1024 * 1024; // set this at 512 GB for now TODO consider implications

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="blobName">A descriptive name that will be the prefix of all segments created</param>
        /// <param name="blockBlobDirectory">the directory containing the block blobs</param>
        /// <param name="pageBlobDirectory">the directory containing the page blobs</param>
        /// <param name="blobManager">the blob manager handling the leases</param>
        /// <param name="underLease">whether this device needs to be protected by the lease</param>
        public AzureStorageDevice(string blobName, CloudBlobDirectory blockBlobDirectory, CloudBlobDirectory pageBlobDirectory, BlobManager blobManager, bool underLease)
            : base($"{blockBlobDirectory}\\{blobName}", PAGE_BLOB_SECTOR_SIZE, Devices.CAPACITY_UNSPECIFIED)
        {
            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.blockBlobDirectory = blockBlobDirectory;
            this.pageBlobDirectory = pageBlobDirectory;
            this.blobName = blobName;
            this.PartitionErrorHandler = blobManager.PartitionErrorHandler;
            this.BlobManager = blobManager;
            this.underLease = underLease;
        }

        public async Task StartAsync()
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.StartAsync Called target={this.pageBlobDirectory.Prefix}{this.blobName}");

            // list all the blobs representing the segments
            var prefix = $"{this.blockBlobDirectory.Prefix}{this.blobName}.";

            BlobContinuationToken continuationToken = null;
            do
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                }
                var response = await this.pageBlobDirectory.ListBlobsSegmentedAsync(useFlatBlobListing: false, blobListingDetails: BlobListingDetails.None, maxResults: 1000,
                    currentToken: continuationToken, options: BlobManager.BlobRequestOptionsWithRetry, operationContext: null)
                    .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);

                foreach (IListBlobItem item in response.Results)
                {
                    if (item is CloudPageBlob pageBlob)
                    {
                        if (Int32.TryParse(pageBlob.Name.Replace(prefix, ""), out int segmentId))
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.StartAsync found segment={pageBlob.Name}");

                            bool ret = this.blobs.TryAdd(segmentId, new BlobEntry(pageBlob, this));

                            if (!ret)
                            {
                                throw new InvalidOperationException("Recovery of blobs is single-threaded and should not yield any failure due to concurrency");
                            }
                        }
                    }
                }
                continuationToken = response.ContinuationToken;
            }
            while (continuationToken != null);

            // find longest contiguous sequence at end
            var keys = this.blobs.Keys.ToList();
            if (keys.Count == 0)
            {
                // nothing has been written to this device so far.
                this.startSegment = 0;
                this.endSegment = -1;
            }
            else
            {
                keys.Sort();
                this.endSegment = keys.Last();
                for (int i = keys.Count - 2; i >= 0; i--)
                {
                    if (keys[i] == keys[i + 1] - 1)
                    {
                        this.startSegment = i;
                    }
                }
            }

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.StartAsync determined segment range for {this.pageBlobDirectory.Prefix}{this.blobName}: start={this.startSegment} end={this.endSegment}");
        }

        /// <summary>
        /// Is called on exceptions, if non-null; can be set by application
        /// </summary>
        internal BlobManager BlobManager { get; set; }

        string GetSegmentBlobName(int segmentId)
        {
            return $"{this.blobName}.{segmentId}";
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="StorageDeviceBase.Dispose">Inherited</see>
        /// </summary>
        public override void Dispose()
        {
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            if (this.blobs.TryRemove(segment, out BlobEntry blob))
            {
                CloudPageBlob pageBlob = blob.PageBlob;

                if (this.underLease)
                {
                    this.BlobManager.ConfirmLeaseIsGoodForAWhile();
                }

                if (!this.PartitionErrorHandler.IsTerminated)
                {
                    pageBlob.DeleteAsync(cancellationToken: this.PartitionErrorHandler.Token)
                       .ContinueWith((Task t) =>
                       {
                           if (t.IsFaulted)
                           {
                               this.BlobManager?.HandleBlobError(nameof(RemoveSegmentAsync), "could not remove page blob for segment", pageBlob?.Name, t.Exception, false, true);
                           }
                           callback(result);
                       });
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.ReadAsync Called segmentId={segmentId} sourceAddress={sourceAddress} readLength={readLength}");

            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!this.blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                var nonLoadedBlob = this.pageBlobDirectory.GetPageBlobReference(this.GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.HandleBlobError(nameof(ReadAsync), exception.Message, nonLoadedBlob?.Name, exception, true, false);
                throw exception;
            }

            this.ReadFromBlobUnsafeAsync(blobEntry.PageBlob, (long)sourceAddress, (long)destinationAddress, readLength)
                  .ContinueWith((Task t) =>
                  {
                      if (t.IsFaulted)
                      {
                          this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.ReadAsync Returned (Failure)");
                          callback(uint.MaxValue, readLength, context);
                      }
                      else
                      {
                          this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.ReadAsync Returned");
                          callback(0, readLength, context);
                      }
                  });
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.WriteAsync Called segmentId={segmentId} destinationAddress={destinationAddress} numBytesToWrite={numBytesToWrite}");

            if (!this.blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                BlobEntry entry = new BlobEntry(this);
                if (this.blobs.TryAdd(segmentId, entry))
                {
                    CloudPageBlob pageBlob = this.pageBlobDirectory.GetPageBlobReference(this.GetSegmentBlobName(segmentId));

                    // If segment size is -1 we use a default
                    var size = this.segmentSize == -1 ? AzureStorageDevice.MAX_PAGEBLOB_SIZE : this.segmentSize;

                    // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                    // After creation is done, we can call write.
                    _ = entry.CreateAsync(size, pageBlob);
                }
                // Otherwise, some other thread beat us to it. Okay to use their blobs.
                blobEntry = this.blobs[segmentId];
            }
            this.TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, callback, context);
        }

        //---- The actual read and write accesses to the page blobs

        unsafe Task WritePortionToBlobUnsafeAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length)
        {
            return this.WritePortionToBlobAsync(new UnmanagedMemoryStream((byte*)sourceAddress + offset, length), blob, sourceAddress, destinationAddress, offset, length);
        }

        async Task WritePortionToBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length)
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.WritePortionToBlobAsync Called target={blob.Name} length={length} destinationAddress={destinationAddress + offset}");
            var stopwatch = new Stopwatch();

            try
            {
                await BlobManager.AsynchronousStorageWriteMaxConcurrency.WaitAsync();

                int numAttempts = 0;
                long streamPosition = stream.Position;

                while(true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (this.underLease)
                        {
                            await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                        }

                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"starting upload target={blob.Name} length={length} destinationAddress={destinationAddress + offset} attempt={numAttempts}");
                        stopwatch.Restart();

                        if (length > 0)
                        {
                            var blobRequestOptions = numAttempts > 2 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;

                            await blob.WritePagesAsync(stream, destinationAddress + offset,
                                contentChecksum: null, accessCondition: null, options: blobRequestOptions, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token)
                                .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
                        }

                        stopwatch.Stop();

                        if (stopwatch.ElapsedMilliseconds > 1000 + length / 1000)
                        {
                            this.BlobManager?.TraceHelper.FasterPerfWarning($"CloudPageBlob.WritePagesAsync took {stopwatch.ElapsedMilliseconds:f1}ms, which is excessive; target={blob.Name} length={length} destinationAddress={destinationAddress + offset}");
                        }
                        break;
                    }
                    catch (StorageException e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();
                        if (BlobUtils.IsTimeout(e))
                        {
                            this.BlobManager?.TraceHelper.FasterPerfWarning($"CloudPageBlob.WritePagesAsync timed out after {stopwatch.ElapsedMilliseconds:f1}ms, retrying now; numAttempts={numAttempts} target={blob.Name} length={length} destinationAddress={destinationAddress + offset}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.BlobManager?.HandleBlobError(nameof(WritePortionToBlobAsync), $"could not write to page blob, will retry in {nextRetryIn}s, numAttempts={numAttempts}", blob?.Name, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        stream.Seek(streamPosition, SeekOrigin.Begin); // must go back to original position before retry
                        continue;
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        this.BlobManager?.HandleBlobError(nameof(WritePortionToBlobAsync), $"could not write to page blob target={blob.Name} length={length} destinationAddress={destinationAddress + offset}", blob?.Name, exception, true, this.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                };
            }
            finally
            {
                BlobManager.AsynchronousStorageWriteMaxConcurrency.Release();
                stream.Dispose();
            }
        }


        unsafe Task ReadFromBlobUnsafeAsync(CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, destinationAddress, readLength);
        }

        async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.ReadFromBlobAsync Called target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");
            var stopwatch = new Stopwatch();

            try
            {
                await BlobManager.AsynchronousStorageReadMaxConcurrency.WaitAsync();

                int numAttempts = 0;

                while (true) // retry loop
                {
                    numAttempts++;         
                    try
                    {
                        if (this.underLease)
                        {
                            await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                        }

                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"starting download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress} attempt={numAttempts}");
                        stopwatch.Restart();

                        if (readLength > 0)
                        {
                            var blobRequestOptions = (numAttempts > 1 || readLength > MAX_UPLOAD_SIZE)
                                ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;

                            await blob
                                .DownloadRangeToStreamAsync(stream, sourceAddress, readLength, accessCondition: null, options: blobRequestOptions, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token)
                                .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
                        }

                        stopwatch.Stop();
                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"finished download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1}");

                        if (stopwatch.ElapsedMilliseconds > 1000 + readLength / 1000)
                        {
                            this.BlobManager?.TraceHelper.FasterPerfWarning($"CloudPageBlob.DownloadRangeToStreamAsync took {stopwatch.ElapsedMilliseconds / 1000}s, which is excessive; target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");
                        }

                        if (stream.Position != readLength)
                        {
                            throw new InvalidDataException($"wrong amount of data received from page blob, expected={readLength}, actual={stream.Position}");
                        }
                        break;
                    }
                    catch (StorageException e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();
                        if (BlobUtils.IsTimeout(e))
                        {
                            this.BlobManager?.TraceHelper.FasterPerfWarning($"CloudPageBlob.DownloadRangeToStreamAsync timed out after {stopwatch.ElapsedMilliseconds:f1}ms, retrying now; numAttempts={numAttempts} target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.BlobManager?.HandleBlobError(nameof(ReadFromBlobAsync), $"could not read from page blob, will retry in {nextRetryIn}s, numAttempts={numAttempts}", blob?.Name, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        stream.Seek(0, SeekOrigin.Begin); // must go back to original position before retrying
                        continue;
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        this.BlobManager?.HandleBlobError(nameof(ReadFromBlobAsync), "could not read from page blob", blob?.Name, exception, true, this.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                }
            }
            finally
            {
                BlobManager.AsynchronousStorageReadMaxConcurrency.Release();
                stream.Dispose();
            }
        }

        void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob == null
                && blobEntry.TryQueueAction(p => this.WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, callback, context)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry.PageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, context);
        }

        unsafe void WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            this.WriteToBlobAsync(blob, sourceAddress, (long)destinationAddress, numBytesToWrite)
                .ContinueWith((Task t) =>
                    {
                        if (t.IsFaulted)
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.WriteAsync Returned (Failure)");
                            callback(uint.MaxValue, numBytesToWrite, context);
                        }
                        else
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.WriteAsync Returned");
                            callback(0, numBytesToWrite, context);
                        }
                    });
        }

        async Task WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite)
        {
            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, MAX_UPLOAD_SIZE);
                await this.WritePortionToBlobUnsafeAsync(blob, sourceAddress, destinationAddress, offset, length).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}
