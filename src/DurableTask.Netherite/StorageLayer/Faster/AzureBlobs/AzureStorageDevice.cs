﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Models;
    using DurableTask.Core.Common;
    using FASTER.core;

    /// <summary>
    /// A IDevice Implementation that is backed by<see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
    /// This device is slower than a local SSD or HDD, but provides scalability and shared access in the cloud.
    /// </summary>
    class AzureStorageDevice : StorageDeviceBase
    {
        readonly ConcurrentDictionary<int, BlobEntry> blobs;
        readonly BlobUtilsV12.BlobDirectory blockBlobDirectory;
        readonly BlobUtilsV12.BlobDirectory pageBlobDirectory;
        readonly string blobName;
        readonly bool underLease;
        readonly ConcurrentDictionary<long, ReadWriteRequestInfo> pendingReadWriteOperations;
        readonly ConcurrentDictionary<long, RemoveRequestInfo> pendingRemoveOperations;
        readonly Timer hangCheckTimer;
        readonly SemaphoreSlim singleWriterSemaphore;
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

        public SemaphoreSlim SingleWriterSemaphore => this.singleWriterSemaphore;

        internal IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        // Azure Page Blobs have a fixed sector size of 512 bytes.
        const uint PAGE_BLOB_SECTOR_SIZE = 512;
        // Max upload size must be at most 4MB
        // we use an even smaller value to improve retry/timeout behavior in highly contended situations
        // Also, this allows us to use aggressive timeouts to kill stragglers
        const uint MAX_UPLOAD_SIZE = 1024 * 1024;
        const uint MAX_DOWNLOAD_SIZE = 1024 * 1024;

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="blobName">A descriptive name that will be the prefix of all segments created</param>
        /// <param name="blockBlobDirectory">the directory containing the block blobs</param>
        /// <param name="pageBlobDirectory">the directory containing the page blobs</param>
        /// <param name="blobManager">the blob manager handling the leases</param>
        /// <param name="underLease">whether this device needs to be protected by the lease</param>
        public AzureStorageDevice(string blobName, BlobUtilsV12.BlobDirectory blockBlobDirectory, BlobUtilsV12.BlobDirectory pageBlobDirectory, BlobManager blobManager, bool underLease)
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
            this.hangCheckTimer = new Timer(this.DetectHangs, null, 0, 20000);
            this.singleWriterSemaphore = underLease ? new SemaphoreSlim(1) : null;
            this.limit = TimeSpan.FromSeconds(90);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"AzureStorageDevice {this.blockBlobDirectory}{this.blobName}";
        }

        public async Task StartAsync()
        {
            try
            {
                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.StartAsync target={this.pageBlobDirectory}{this.blobName}");

                // list all the blobs representing the segments
                var prefix = $"{this.blockBlobDirectory}{this.blobName}.";

                string continuationToken = null;
                IEnumerable<BlobItem> pageResults = null;

                do
                {
                    await this.BlobManager.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageReadMaxConcurrency,
                        this.underLease,
                        "BlobContainerClient.GetBlobsAsync",
                        "RecoverDevice",
                        $"continuationToken={continuationToken}",
                        this.pageBlobDirectory.ToString(),
                        2000,
                        true,
                        async (numAttempts) =>
                        {
                            var client = this.pageBlobDirectory.Client.WithRetries;

                            var enumerator = client.GetBlobsAsync(
                                prefix: prefix,
                                cancellationToken: this.PartitionErrorHandler.Token)
                                .AsPages(continuationToken, 100)
                                .GetAsyncEnumerator(cancellationToken: this.PartitionErrorHandler.Token);

                            if (await enumerator.MoveNextAsync())
                            {
                                var page = enumerator.Current;
                                pageResults = page.Values;
                                continuationToken = page.ContinuationToken;
                                return page.Values.Count; // not accurate, in terms of bytes, but still useful for tracing purposes
                            }
                            else
                            {
                                pageResults = Enumerable.Empty<BlobItem>();
                                continuationToken = null;
                                return 0;
                            };
                        });

                    foreach (var item in pageResults)
                    {
                        if (Int32.TryParse(item.Name.Replace(prefix, ""), out int segmentId))
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.StartAsync found segment={item.Name}");

                            bool ret = this.blobs.TryAdd(segmentId, new BlobEntry(BlobUtilsV12.GetPageBlobClients(this.pageBlobDirectory.Client, item.Name), item.Properties.ETag.Value, this));

                            if (!ret)
                            {
                                throw new InvalidOperationException("Recovery of blobs is single-threaded and should not yield any failure due to concurrency");
                            }
                        }
                    }
                }
                while (!string.IsNullOrEmpty(continuationToken));

                // make sure we did not lose the lease while iterating to find the blobs
                await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync();
                this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();


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
                    this.endSegment = this.startSegment = keys[keys.Count - 1];
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

        internal void DetectHangs(object _)
        {
            DateTime threshold = DateTime.UtcNow - (Debugger.IsAttached ? TimeSpan.FromMinutes(30) : this.limit);

            foreach (var kvp in this.pendingReadWriteOperations)
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
            this.singleWriterSemaphore?.Dispose();
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
                deletionTask = this.BlobManager.PerformWithRetriesAsync(
                    null,
                    this.underLease,
                    "BlobBaseClient.DeleteAsync",
                    "DeleteDeviceSegment",
                    "",
                    entry.PageBlob.Default.Name,
                    5000,
                    true,
                    async (numAttempts) =>
                    {
                        var client = (numAttempts > 1) ? entry.PageBlob.Default : entry.PageBlob.Aggressive;
                        var response = await client.DeleteIfExistsAsync(cancellationToken: this.PartitionErrorHandler.Token);
                        return response ? 1 : 0;
                    });
            }
                
            deletionTask.ContinueWith((Task t) =>
            {
                if (this.pendingRemoveOperations.TryRemove(id, out var request))
                {
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.RemoveSegmentAsync id={id}");
                    request.Callback(request.Result);
                }
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Delete the device blobs in storage.
        /// </summary>
        /// <returns></returns>
        Task DeleteAsync()
        {
            Task Delete(BlobEntry entry)
            {
                return this.BlobManager.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    this.underLease,
                    "BlobBaseClient.DeleteAsync",
                    "DeleteDevice",
                    "",
                    entry.PageBlob.Default.Name,
                    5000,
                    false,
                    async (numAttempts) =>
                    {
                        var client = (numAttempts > 1) ? entry.PageBlob.Default : entry.PageBlob.Aggressive;
                        try
                        {
                            using var response = await client.DeleteAsync(cancellationToken: this.PartitionErrorHandler.Token);
                            return 1;
                        }
                        catch (Azure.RequestFailedException ex) when (numAttempts > 1 && BlobUtilsV12.BlobDoesNotExist(ex))
                        {
                            // blob may have already been deleted by the previous attempt
                            return 0;
                        }
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
                var nonLoadedBlob = this.pageBlobDirectory.GetPageBlobClient(this.GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.HandleStorageError(nameof(ReadAsync), exception.Message, nonLoadedBlob.Default?.Name, exception, true, false);
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
                  }, TaskContinuationOptions.ExecuteSynchronously);
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
                    var pageBlob = this.pageBlobDirectory.GetPageBlobClient(this.GetSegmentBlobName(segmentId));

                    // If segment size is -1 we use the default starting size for auto-expanding page blobs.
                    var size = this.segmentSize == -1 ? this.BlobManager.StartingPageBlobSize : this.segmentSize;

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

        unsafe Task WritePortionToBlobUnsafeAsync(BlobEntry blobEntry, IntPtr sourceAddress, long destinationAddress, long offset, uint length, long id)
        {
            return this.WritePortionToBlobAsync(new UnmanagedMemoryStream((byte*)sourceAddress + offset, length), blobEntry, sourceAddress, destinationAddress, offset, length, id);
        }

        async Task WritePortionToBlobAsync(UnmanagedMemoryStream stream, BlobEntry blobEntry, IntPtr sourceAddress, long destinationAddress, long offset, uint length, long id)
        {
            using (stream)
            {
                long originalStreamPosition = stream.Position;
                await this.BlobManager.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    true,
                    "PageBlobClient.UploadPagesAsync",
                    "WriteToDevice",
                    $"id={id} length={length} destinationAddress={destinationAddress + offset}",
                    blobEntry.PageBlob.Default.Name,
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
                            var client = numAttempts > 2 ? blobEntry.PageBlob.Default : blobEntry.PageBlob.Aggressive;

                            try
                            {
                                var response = await client.UploadPagesAsync(
                                     content: stream,
                                     offset: destinationAddress + offset,
                                     transactionalContentHash: null,
                                     conditions: this.underLease ? new PageBlobRequestConditions() { IfMatch = blobEntry.ETag } : null,
                                     progressHandler: null,
                                     cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);
                            
                                blobEntry.ETag = response.Value.ETag;
                            }
                            catch (Azure.RequestFailedException e) when (e.ErrorCode == "InvalidPageRange")
                            {
                                // this kind of error can indicate that the page blob is too small.
                                // from the perspective of FASTER, this storage device is infinite, so it may write past the end of the blob.
                                // To deal with this situation, we dynamically enlarge this device as needed.

                                // first, compute desired size to request
                                long currentSize = (await client.GetPropertiesAsync().ConfigureAwait(false)).Value.ContentLength;
                                long sizeToRequest = currentSize; 
                                long sizeToAccommodate = destinationAddress + offset + length + 1;
                                while (sizeToAccommodate > sizeToRequest)
                                {
                                    sizeToRequest <<= 1;
                                }

                                if (sizeToRequest <= currentSize)
                                {
                                    throw e; // blob is already big enough, so this exception was thrown for some other reason
                                }
                                else
                                {
                                    if (sizeToRequest > this.BlobManager.MaxPageBlobSize)
                                    {
                                        throw new InvalidOperationException($"cannot expand page blob {blobEntry.PageBlob.Default.Name} beyond maximum size {this.BlobManager.MaxPageBlobSize}");
                                    }

                                    // enlarge the blob to accommodate the size
                                    await client.ResizeAsync(
                                        sizeToRequest,
                                        conditions: this.underLease ? new PageBlobRequestConditions() { IfMatch = blobEntry.ETag } : null,
                                        cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);

                                    // force retry
                                    // this also generates a warning in the traces, containing the information about what happened 
                                    throw new BlobUtils.ForceRetryException($"page blob was enlarged from {currentSize} to {sizeToRequest}", e);
                                }
                            }
                        }

                        return (long)length;
                    },
                    async () =>
                    {
                        var response = await blobEntry.PageBlob.Default.GetPropertiesAsync();
                        blobEntry.ETag = response.Value.ETag;

                    }).ConfigureAwait(false);
            }
        }

        unsafe Task ReadFromBlobUnsafeAsync(BlobUtilsV12.PageBlobClients blob, long sourceAddress, long destinationAddress, uint readLength, long id)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, readLength, id);
        }

        async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, BlobUtilsV12.PageBlobClients blob, long sourceAddress, uint readLength, long id)
        {
            using (stream)
            {
                // we use this to prevent reading past the end of the page blob
                // but for performance reasons (Azure storage access required to determine current size of page blob)
                // we set it lazily, i.e. only after a request failed
                long? readCap = null;

                long offset = 0;
                while (readLength > 0)
                {
                    // determine how much we are going to try to read in this portion
                    var length = Math.Min(readLength, MAX_DOWNLOAD_SIZE); 
 
                    await this.BlobManager.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageReadMaxConcurrency,
                        true,
                        "PageBlobClient.DownloadStreamingAsync",
                        "ReadFromDevice",
                        $"id={id} readLength={length} sourceAddress={sourceAddress + offset}",
                        blob.Default.Name,
                        1000 + (int)length / 1000,
                        true,
                        async (numAttempts) =>
                        {
                            stream.Seek(offset, SeekOrigin.Begin);

                            long requestedLength = length;

                            if (readCap.HasValue && sourceAddress + offset + requestedLength > readCap.Value)
                            {
                                requestedLength = readCap.Value - (sourceAddress + offset);

                                if (requestedLength <= 0)
                                {
                                    requestedLength = 0;
                                }
                            }

                            if (requestedLength > 0)
                            {
                                var client = (numAttempts > 1 || requestedLength == MAX_DOWNLOAD_SIZE) ? blob.Default : blob.Aggressive;

                                try
                                {
                                    var response = await client.DownloadStreamingAsync(
                                        range: new Azure.HttpRange(sourceAddress + offset, requestedLength),
                                        conditions: null,
                                        rangeGetContentHash: false,
                                        cancellationToken: this.PartitionErrorHandler.Token)
                                        .ConfigureAwait(false);

                                    using (var streamingResult = response.Value)
                                    {
                                        await streamingResult.Content.CopyToAsync(stream).ConfigureAwait(false);
                                    }

                                    // We have observed that we may get 206 (Partial Response) codes where the actual length is less than the requested length
                                    // The Azure storage client SDK handles the http codes transparently, but we may still observe that fewer bytes than
                                    // requested were returned by the streamingResult.
                                    long actualLength = (stream.Position - offset); 

                                    if (actualLength < requestedLength)
                                    {
                                        this.BlobManager.StorageTracer?.FasterStorageProgress($"PageBlob.DownloadStreamingAsync id={id} returned partial response range={response.Value.Details.ContentRange} requestedLength={requestedLength} actualLength={actualLength}");
                                       
                                        if (actualLength == 0)
                                        {
                                            throw new InvalidDataException($"PageBlob.DownloadStreamingAsync returned empty response, range={response.Value.Details.ContentRange} requestedLength={requestedLength} ");
                                        }
                                        else if (actualLength % 512 != 0)
                                        {
                                            throw new InvalidDataException($"PageBlob.DownloadStreamingAsync returned unaligned response, range={response.Value.Details.ContentRange} requestedLength={requestedLength} actualLength=${actualLength}");
                                        }
                                        else
                                        {
                                            length = (uint)actualLength; // adjust length to actual length read so the next read will start where this read ended
                                        }
                                    }
                                    else if (actualLength > requestedLength)
                                    {
                                        throw new InvalidDataException($"PageBlob.DownloadStreamingAsync returned too much data, range={response.Value.Details.ContentRange} requestedLength={requestedLength} actualLength=${actualLength}");
                                    }
                                }
                                catch (Azure.RequestFailedException e) when (e.ErrorCode == "InvalidRange")
                                {
                                    // from the perspective of FASTER, this storage device is infinite, so it may read past the end of the blob.
                                    // But even though it requests more data than what it wrote, it will only actually use what it wrote before. 
                                    // so we can deal with this situation by just copying fewer bytes from the blob into the buffer.

                                    // first, determine current page blob size.
                                    var properties = await client.GetPropertiesAsync().ConfigureAwait(false);
                                    readCap = properties.Value.ContentLength;

                                    if (sourceAddress + offset + requestedLength <= readCap.Value)
                                    {
                                        // page blob is big enough, so this exception was thrown for some other reason
                                        throw e; 
                                    }
                                    else
                                    {
                                        // page blob was indeed too small; now that we have set a read cap, force a retry
                                        // so we can read an adjusted portion
                                        throw new BlobUtils.ForceRetryException($"reads now capped at {readCap}", e);
                                    }
                                }                  
                            }

                            return length;
                        });

                    // adjust how much we have to read, and where to read from, in the next iteration
                    // based on how much was actually read in this iteration.
                    readLength -= length;
                    offset += length;
                }
            }
        }

        void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, long id)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob.Default == null
                && blobEntry.TryQueueAction(() => this.WriteToBlobAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, id)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, id);
        }

        unsafe void WriteToBlobAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, long id)
        {
            this.WriteToBlobAsync(blobEntry, sourceAddress, (long)destinationAddress, numBytesToWrite, id)
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

                        if (this.underLease)
                        {
                            this.SingleWriterSemaphore.Release();
                        }

                    }, TaskContinuationOptions.ExecuteSynchronously);
        }

        async Task WriteToBlobAsync(BlobEntry blobEntry, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite, long id)
        {
            if (this.underLease)
            {
                await this.SingleWriterSemaphore.WaitAsync();
            }

            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, MAX_UPLOAD_SIZE);
                await this.WritePortionToBlobUnsafeAsync(blobEntry, sourceAddress, destinationAddress, offset, length, id).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}
