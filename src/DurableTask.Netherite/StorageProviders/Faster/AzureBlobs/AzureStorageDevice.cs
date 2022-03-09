// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
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
        readonly ConcurrentDictionary<long, ReadWriteRequestInfo> pendingReadWriteOperations;
        readonly ConcurrentDictionary<long, RemoveRequestInfo> pendingRemoveOperations;
        readonly Timer hangCheckTimer;
        readonly TimeSpan limit;

        static long sequenceNumber;

        struct ReadWriteRequestInfo
        {
            public bool IsRead;
            public DeviceIOCompletionCallback Callback;
            public uint NumBytes;
            public object Context;
            public DateTime TimeStamp;
        }

        struct RemoveRequestInfo
        {
            public AsyncCallback Callback;
            public IAsyncResult Result;
            public DateTime TimeStamp;
        }

        internal IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        // Azure Page Blobs have a fixed sector size of 512 bytes.
        const uint PAGE_BLOB_SECTOR_SIZE = 512;
        // Max upload size must be at most 4MB
        // we use an even smaller value to improve retry/timeout behavior in highly contended situations
        // Also, this allows us to use aggressive timeouts to kill stragglers
        const uint MAX_UPLOAD_SIZE = 1024 * 1024;
        const uint MAX_DOWNLOAD_SIZE = 1024 * 1024;

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
            this.pendingReadWriteOperations = new ConcurrentDictionary<long, ReadWriteRequestInfo>();
            this.pendingRemoveOperations = new ConcurrentDictionary<long, RemoveRequestInfo>();
            this.blockBlobDirectory = blockBlobDirectory;
            this.pageBlobDirectory = pageBlobDirectory;
            this.blobName = blobName;
            this.PartitionErrorHandler = blobManager.PartitionErrorHandler;
            this.PartitionErrorHandler.Token.Register(this.CancelAllRequests);
            this.BlobManager = blobManager;
            this.underLease = underLease;
            this.hangCheckTimer = new Timer(this.CheckForHangs, null, 0, 20000);
            this.limit = TimeSpan.FromSeconds(60);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"AzureStorageDevice {this.blockBlobDirectory.Prefix}{this.blobName}";
        }

        public async Task StartAsync()
        {
            try
            {
                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.StartAsync target={this.pageBlobDirectory.Prefix}{this.blobName}");

                // list all the blobs representing the segments
                var prefix = $"{this.blockBlobDirectory.Prefix}{this.blobName}.";

                BlobContinuationToken continuationToken = null;
                do
                {
                    if (this.underLease)
                    {
                        await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                    }

                    BlobResultSegment response = null;

                    await this.BlobManager.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageReadMaxConcurrency,
                        true,
                        "PageBlobDirectory.ListBlobsSegmentedAsync",
                        "RecoverDevice",
                        $"continuationToken={continuationToken}",
                        this.pageBlobDirectory.Prefix,
                        2000,
                        true,
                        async (numAttempts) =>
                        {
                            response = await this.pageBlobDirectory.ListBlobsSegmentedAsync(
                                useFlatBlobListing: false,
                                blobListingDetails: BlobListingDetails.None,
                                maxResults: 100,
                                currentToken: continuationToken,
                                options: BlobManager.BlobRequestOptionsWithRetry,
                                operationContext: null);
                            return response.Results.Count(); // not accurate, in terms of bytes, but still useful for tracing purposes
                        });

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

                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.StartAsync, determined segment range for {this.pageBlobDirectory.Prefix}{this.blobName}: start={this.startSegment} end={this.endSegment}");
            }
            catch
            {
                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.StartAsync failed");
                throw;
            }
        }


        /// <summary>
        /// Is called on exceptions, if non-null; can be set by application
        /// </summary>
        internal BlobManager BlobManager { get; set; }

        string GetSegmentBlobName(int segmentId)
        {
            return $"{this.blobName}.{segmentId}";
        }

        internal void CheckForHangs(object _)
        {
            DateTime threshold = DateTime.UtcNow - this.limit;

            foreach(var kvp in this.pendingReadWriteOperations)
            {
                if (kvp.Value.TimeStamp < threshold)
                {
                    this.BlobManager.PartitionErrorHandler.HandleError("DetectHangs", $"storage operation id={kvp.Key} has exceeded the time limit {this.limit}", null, true, false);
                    return;
                }
            }
            foreach (var kvp in this.pendingRemoveOperations)
            {
                if (kvp.Value.TimeStamp < threshold)
                {
                    this.BlobManager.PartitionErrorHandler.HandleError("DetectHangs", $"storage operation id={kvp.Key} has exceeded the time limit {this.limit}", null, true, false);
                    return;
                }
            }
        }

        void CancelAllRequests()
        {
            foreach (var id in this.pendingReadWriteOperations.Keys.ToList())
            {
                if (this.pendingReadWriteOperations.TryRemove(id, out var request))
                {
                    if (request.IsRead)
                    {
                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.ReadAsync id={id} (Canceled)");
                    }
                    else
                    {
                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.WriteAsync id={id} (Canceled)");
                    }
                    request.Callback(uint.MaxValue, request.NumBytes, request.Context);
                }
            }
            foreach (var id in this.pendingRemoveOperations.Keys.ToList())
            {
                if (this.pendingRemoveOperations.TryRemove(id, out var request))
                {
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.RemoveSegmentAsync id={id} (Canceled)");
                    request.Callback(request.Result);
                }
            }
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="StorageDeviceBase.Dispose">Inherited</see>
        /// </summary>
        public override void Dispose()
        {
            this.hangCheckTimer.Dispose();
        }
 
        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            long id = Interlocked.Increment(ref AzureStorageDevice.sequenceNumber);

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.RemoveSegmentAsync id={id} segment={segment}");

            this.pendingRemoveOperations.TryAdd(id, new RemoveRequestInfo()
            {
                Callback = callback,
                Result = result,
                TimeStamp = DateTime.UtcNow
            });

            Task deletionTask = Task.CompletedTask;

            if (this.blobs.TryRemove(segment, out BlobEntry entry))
            {
                CloudPageBlob pageBlob = entry.PageBlob;
                deletionTask = this.BlobManager.PerformWithRetriesAsync(
                    null,
                    this.underLease,
                    "CloudPageBlob.DeleteAsync",
                    "DeleteDeviceSegment",
                    "",
                    pageBlob.Name,
                    5000,
                    true,
                    async (numAttempts) =>
                    {
                        await pageBlob.DeleteAsync(cancellationToken: this.PartitionErrorHandler.Token);
                        return 1;
                    });
            }
                
            deletionTask.ContinueWith((Task t) =>
            {
                if (this.pendingRemoveOperations.TryRemove(id, out var request))
                {
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.RemoveSegmentAsync id={id}");
                    request.Callback(request.Result);
                }
            });
        }

        /// <summary>
        /// Delete the device blobs in storage.
        /// </summary>
        /// <returns></returns>
        Task DeleteAsync()
        {
            Task Delete(BlobEntry entry)
            {
                CloudPageBlob pageBlob = entry.PageBlob;
                return this.BlobManager.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    this.underLease,
                    "CloudPageBlob.DeleteAsync",
                    "DeleteDevice",
                    "",
                    pageBlob.Name,
                    5000,
                    false,
                    async (numAttempts) =>
                    { 
                        await pageBlob.DeleteIfExistsAsync(cancellationToken: this.PartitionErrorHandler.Token);
                        return 1;
                    });
            }

           return Task.WhenAll(this.blobs.Values.Select(Delete).ToList());
        }

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            long id = Interlocked.Increment(ref AzureStorageDevice.sequenceNumber);

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.ReadAsync id={id} segmentId={segmentId} sourceAddress={sourceAddress} readLength={readLength}");

            this.pendingReadWriteOperations.TryAdd(id, new ReadWriteRequestInfo()
            {
                IsRead = true,
                Callback = callback,
                NumBytes = readLength,
                Context = context,
                TimeStamp = DateTime.UtcNow
            });

            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!this.blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                var nonLoadedBlob = this.pageBlobDirectory.GetPageBlobReference(this.GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.HandleStorageError(nameof(ReadAsync), exception.Message, nonLoadedBlob?.Name, exception, true, false);
                throw exception;
            }

            this.ReadFromBlobUnsafeAsync(blobEntry.PageBlob, (long)sourceAddress, (long)destinationAddress, readLength, id)
                  .ContinueWith((Task t) =>
                  {
                      if (this.pendingReadWriteOperations.TryRemove(id, out ReadWriteRequestInfo request))
                      {
                          if (t.IsFaulted)
                          {
                              this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.ReadAsync id={id} (Failure)");
                              request.Callback(uint.MaxValue, request.NumBytes, request.Context);
                          }
                          else
                          {
                              this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.ReadAsync id={id}");
                              request.Callback(0, request.NumBytes, request.Context);
                          }
                      }
                  });
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            long id = Interlocked.Increment(ref AzureStorageDevice.sequenceNumber);

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.WriteAsync id={id} segmentId={segmentId} destinationAddress={destinationAddress} numBytesToWrite={numBytesToWrite}");

            this.pendingReadWriteOperations.TryAdd(id, new ReadWriteRequestInfo()
            {
                IsRead = false,
                Callback = callback,
                NumBytes = numBytesToWrite,
                Context = context,
                TimeStamp = DateTime.UtcNow
            });

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
            this.TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, id);
        }

        //---- The actual read and write accesses to the page blobs

        unsafe Task WritePortionToBlobUnsafeAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length, long id)
        {
            return this.WritePortionToBlobAsync(new UnmanagedMemoryStream((byte*)sourceAddress + offset, length), blob, sourceAddress, destinationAddress, offset, length, id);
        }

        async Task WritePortionToBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length, long id)
        {
            using (stream)
            {
                long originalStreamPosition = stream.Position;
                await this.BlobManager.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    true,
                    "CloudPageBlob.WritePagesAsync",
                    "WriteToDevice",
                    $"id={id} length={length} destinationAddress={destinationAddress + offset}",
                    blob.Name,
                    1000 + (int)length / 1000,
                    true,
                    async (numAttempts) =>
                    {
                        if (numAttempts > 0)
                        {
                            stream.Seek(originalStreamPosition, SeekOrigin.Begin); // must go back to original position before retry
                        }

                        if (length > 0)
                        {
                            var blobRequestOptions = numAttempts > 2 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;

                            await blob.WritePagesAsync(stream, destinationAddress + offset,
                                contentChecksum: null, accessCondition: null, options: blobRequestOptions, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token)
                                .ConfigureAwait(false);
                        }

                        return (long)length;
                    });
            }
        }

        unsafe Task ReadFromBlobUnsafeAsync(CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength, long id)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, readLength, id);
        }

        async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, long sourceAddress, uint readLength, long id)
        {
            using (stream)
            {
                long offset = 0;
                while (readLength > 0)
                {
                    var length = Math.Min(readLength, MAX_DOWNLOAD_SIZE);

                    await this.BlobManager.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageReadMaxConcurrency,
                        true,
                        "CloudPageBlob.DownloadRangeToStreamAsync",
                        "ReadFromDevice",
                        $"id={id} readLength={length} sourceAddress={sourceAddress + offset}",
                        blob.Name,
                        1000 + (int)length / 1000,
                        true,
                        async (numAttempts) =>
                        {
                            if (numAttempts > 0)
                            {
                                stream.Seek(offset, SeekOrigin.Begin); // must go back to original position before retrying
                            }

                            if (length > 0)
                            {
                                var blobRequestOptions = (numAttempts > 1 || length == MAX_DOWNLOAD_SIZE)
                                    ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;

                                await blob
                                    .DownloadRangeToStreamAsync(stream, sourceAddress + offset, length, accessCondition: null, options: blobRequestOptions, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token)
                                    .ConfigureAwait(false);
                            }

                            if (stream.Position != offset + length)
                            {
                                throw new InvalidDataException($"wrong amount of data received from page blob, expected={length}, actual={stream.Position}");
                            }

                            return length;
                        });

                    readLength -= length;
                    offset += length;
                }
            }
        }

        void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, long id)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob == null
                && blobEntry.TryQueueAction(p => this.WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, id)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry.PageBlob, sourceAddress, destinationAddress, numBytesToWrite, id);
        }

        unsafe void WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, long id)
        {
            this.WriteToBlobAsync(blob, sourceAddress, (long)destinationAddress, numBytesToWrite, id)
                .ContinueWith((Task t) =>
                    {
                        if (this.pendingReadWriteOperations.TryRemove(id, out ReadWriteRequestInfo request))
                        {
                            if (t.IsFaulted)
                            {
                                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.WriteAsync id={id} (Failure)");
                                request.Callback(uint.MaxValue, request.NumBytes, request.Context);
                            }
                            else
                            {
                                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.WriteAsync id={id}");
                                request.Callback(0, request.NumBytes, request.Context);
                            }
                        }
                    });
        }

        async Task WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite, long id)
        {
            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, MAX_UPLOAD_SIZE);
                await this.WritePortionToBlobUnsafeAsync(blob, sourceAddress, destinationAddress, offset, length, id).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}
