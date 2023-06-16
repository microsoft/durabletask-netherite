// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.Netherite.Faster;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Azure;
    using Microsoft.Extensions.Logging;

    class BlobBatchReceiver<TEvent> where TEvent : Event
    {
        readonly string traceContext;
        readonly EventHubsTraceHelper traceHelper;
        readonly EventHubsTraceHelper lowestTraceLevel;
        readonly BlobContainerClient containerClient;
        readonly bool keepUntilConfirmed;

        // Event Hubs discards messages after 24h, so we can throw away batches that are older than that
        readonly static TimeSpan expirationTimeSpan = TimeSpan.FromHours(24) + TimeSpan.FromMinutes(1);

        BlobDeletions blobDeletions;

        public BlobBatchReceiver(string traceContext, EventHubsTraceHelper traceHelper, NetheriteOrchestrationServiceSettings settings, bool keepUntilConfirmed)
        {
            this.traceContext = traceContext;
            this.traceHelper = traceHelper;
            this.lowestTraceLevel = traceHelper.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace) ? traceHelper : null;
            var serviceClient = BlobUtilsV12.GetServiceClients(settings.BlobStorageConnection).WithRetries;
            string containerName = BlobManager.GetContainerName(settings.HubName);
            this.containerClient = serviceClient.GetBlobContainerClient(containerName);
            this.keepUntilConfirmed = keepUntilConfirmed;
            this.blobDeletions = this.keepUntilConfirmed ? new BlobDeletions(this) : null;
        }

        public async IAsyncEnumerable<(EventData eventData, TEvent[] events, long)> ReceiveEventsAsync(
            byte[] taskHubGuid, 
            IEnumerable<EventData> hubMessages,
            [EnumeratorCancellation] CancellationToken token,
            long? nextPacketToReceive = null)
        {
            foreach (var eventData in hubMessages)
            {
                var seqno = eventData.SystemProperties.SequenceNumber;

                if (nextPacketToReceive.HasValue)
                {
                    if (seqno < nextPacketToReceive.Value)
                    {
                        this.lowestTraceLevel?.LogTrace("{context} discarded packet #{seqno} because it is already processed", this.traceContext, seqno);
                        continue;
                    }
                    else if (seqno > nextPacketToReceive.Value)
                    {
                        this.traceHelper.LogError("{context} received wrong packet, #{seqno} instead of #{expected} ", this.traceContext, seqno, nextPacketToReceive.Value);
                        // this should never happen, as EventHubs guarantees in-order delivery of packets
                        throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                    }
                }
                
                TEvent evt;
                Packet.BlobReference blobReference;

                try
                {
                    Packet.Deserialize(eventData.Body, out evt, out blobReference, taskHubGuid);
                }
                catch (Exception)
                {
                    this.traceHelper.LogError("{context} could not deserialize packet #{seqno} ({size} bytes)", this.traceContext, seqno, eventData.Body.Count);
                    throw;
                }

                if (blobReference == null)
                {
                    if (evt == null)
                    {
                        this.traceHelper.LogWarning("{context} ignored packet #{seqno} for different taskhub", this.traceContext, seqno);
                    }
                    else
                    {
                        yield return (eventData, new TEvent[1] { evt }, seqno);
                    }
                }
                else // we have to read messages from a blob batch
                {
                    string blobPath = $"{BlobBatchSender.PathPrefix}{blobReference.BlobName}";

                    BlockBlobClient blobClient = this.containerClient.GetBlockBlobClient(blobPath);

                    await BlobManager.AsynchronousStorageReadMaxConcurrency.WaitAsync();

                    byte[] blobContent;

                    token.ThrowIfCancellationRequested();

                    try
                    {
                        this.lowestTraceLevel?.LogTrace("{context} downloading blob {blobName}", this.traceContext, blobClient.Name);

                        Azure.Response<BlobDownloadResult> downloadResult = await blobClient.DownloadContentAsync(token);
                        blobContent = downloadResult.Value.Content.ToArray();

                        this.lowestTraceLevel?.LogTrace("{context} downloaded blob {blobName} ({size} bytes, {count} packets)", this.traceContext, blobClient.Name, blobContent.Length, blobReference.PacketOffsets.Count + 1);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        // normal during shutdown
                        throw;
                    }
                    catch (Exception exception)
                    {
                        this.traceHelper.LogError("{context} failed to read blob {blobName} for #{seqno}: {exception}", this.traceContext, blobClient.Name, seqno, exception);
                        throw;
                    }
                    finally
                    {
                        BlobManager.AsynchronousStorageReadMaxConcurrency.Release();
                    }

                    TEvent[] result = new TEvent[blobReference.PacketOffsets.Count + 1];
                    for (int i = 0; i < result.Length; i++)
                    {
                        var offset = i == 0 ? 0 : blobReference.PacketOffsets[i - 1];
                        var nextOffset = i < blobReference.PacketOffsets.Count ? blobReference.PacketOffsets[i] : blobContent.Length;
                        var length = nextOffset - offset;
                        using var m = new MemoryStream(blobContent, offset, length, writable: false);

                        token.ThrowIfCancellationRequested();

                        try
                        {
                            Packet.Deserialize(m, out result[i], out _, null); // no need to check task hub match again
                        }
                        catch (Exception)
                        {
                            this.traceHelper.LogError("{context} could not deserialize packet from blob {blobName} at #{seqno}.{subSeqNo} offset={offset} length={length}", this.traceContext, blobClient.Name, seqno, i, offset, length);
                            throw;
                        }
                    }
                    yield return (eventData, result, seqno);

                    // we issue a deletion task; there is no strong guarantee that deletion will successfully complete
                    // which means some blobs can be left behind temporarily.
                    // This is fine because we have a second deletion path, a scan that removes old 
                    // blobs that are past EH's expiration date.

                    if (!this.keepUntilConfirmed)
                    {
                        var bgTask = Task.Run(() => this.DeleteBlobAsync(new BlockBlobClient[1] { blobClient }));
                    }
                    else
                    {
                        // we cannot delete the blob until the partition has persisted an input queue position
                        // past this blob, so that we know we will not need to read it again
                        if (this.blobDeletions.TryRegister(result, blobClient))
                        {
                            this.blobDeletions = new BlobDeletions(this);
                        }
                        else
                        {
                            // the current batch will be registered along with the next
                            // batch that successfully registers
                        }
                    }
                }

                if (nextPacketToReceive.HasValue)
                {
                    nextPacketToReceive = seqno + 1;
                }
            }
        }

        public async Task<int> DeleteBlobAsync(IEnumerable<BlockBlobClient> blobClients)
        {
            int deletedCount = 0;

            foreach (var blobClient in blobClients)
            {
                await BlobManager.AsynchronousStorageWriteMaxConcurrency.WaitAsync();

                try
                {
                    this.lowestTraceLevel?.LogTrace("{context} deleting blob {blobName}", this.traceContext, blobClient.Name);
                    Azure.Response response = await blobClient.DeleteAsync();
                    this.lowestTraceLevel?.LogTrace("{context} deleted blob {blobName}", this.traceContext, blobClient.Name);
                    deletedCount++;
                }
                catch (Azure.RequestFailedException e) when (BlobUtilsV12.BlobDoesNotExist(e))
                {
                    this.lowestTraceLevel?.LogTrace("{context} blob {blobName} was already deleted", this.traceContext, blobClient.Name);
                }
                catch (Exception exception)
                {
                    this.traceHelper.LogError("{context} failed to delete blob {blobName} : {exception}", this.traceContext, blobClient.Name, exception);
                }
                finally
                {
                    BlobManager.AsynchronousStorageWriteMaxConcurrency.Release();
                }
            }

            return deletedCount;
        }

        class BlobDeletions : TransportAbstraction.IDurabilityListener
        {
            readonly BlobBatchReceiver<TEvent> blobBatchReceiver;
            readonly List<BlockBlobClient> blobClients;

            public BlobDeletions(BlobBatchReceiver<TEvent> blobBatchReceiver)
            {
                this.blobBatchReceiver = blobBatchReceiver;
                this.blobClients = new List<BlockBlobClient>();
            }

            public bool TryRegister(TEvent[] events, BlockBlobClient blobClient)
            {
                this.blobClients.Add(blobClient);

                for(int i = events.Length - 1; i >= 0; i--)
                {
                    if (events[i] is PartitionUpdateEvent e)
                    {
                        // we can register a callback 
                        // to be invoked after the event has been persisted in the log
                        DurabilityListeners.Register(e, this);
                        return true;
                    }
                }

                return false; // only read or query events in this batch, cannot register a callback
            }

            public void ConfirmDurable(Event evt)
            {
                Task.Run(() => this.blobBatchReceiver.DeleteBlobAsync(this.blobClients));
            }
        }

        public async Task<int> RemoveGarbageAsync(CancellationToken token)
        {
            async IAsyncEnumerable<Azure.Page<BlobItem>> GetExpiredBlobs()
            {
                // use a small first page since most of the time the query will 
                // return blobs that have not expired yet, so we are wasting time and space if the
                // page is large
                var firstpage = await this.containerClient.GetBlobsAsync(
                            prefix: BlobBatchSender.PathPrefix,
                            cancellationToken: token)
                            .AsPages(continuationToken: null, pageSizeHint: 5)
                            .FirstAsync();

                yield return firstpage;

                if (firstpage.ContinuationToken != null)
                {
                    // for the remaining pages, use regular page size to reduce cost
                    var remainingPages = this.containerClient.GetBlobsAsync(
                            prefix: BlobBatchSender.PathPrefix,
                            cancellationToken: token)
                            .AsPages(continuationToken: firstpage.ContinuationToken, pageSizeHint: 100);

                    await foreach (var page in remainingPages)
                    {
                        yield return page;
                    }
                }
            }

            int deletedCount = 0;
            try
            {
                await foreach (Azure.Page<BlobItem> page in GetExpiredBlobs())
                {
                    List<BlockBlobClient> blobs = new List<BlockBlobClient>();
                    bool completed = false;

                    foreach (var blob in page.Values)
                    {
                        if (IsExpired(blob.Name))
                        {
                            blobs.Add(this.containerClient.GetBlockBlobClient(blob.Name));
                        }
                        else
                        {
                            // blobs are sorted in ascending time order, so once we found one that is not
                            // expired yet we can stop enumerating
                            completed = true;
                        }

                        deletedCount += await this.DeleteBlobAsync(blobs);

                        if (completed)
                        {
                            return deletedCount;
                        }

                        bool IsExpired(string path)
                        {
                            // {PathPrefix}2023-06-13T23:28:55.5043743Z-2CA224EC
                            var name = path.Substring(BlobBatchSender.PathPrefix.Length);
                            // 2023-06-13T23:28:55.5043743Z-2CA224EC
                            var date = name.Substring(0, name.Length - 9);
                            // 2023-06-13T23:28:55.5043743Z

                            if (DateTime.TryParse(date, out DateTime result))
                            {
                               return (DateTime.Now - result) > expirationTimeSpan;
                            }
                            else
                            {
                                this.traceHelper.LogError("{context} failed to parse blob name {blobName} : '{date}' is not a DateTime", this.traceContext, name, date);
                                return false;
                            }
                        }
                    }
                }

            }
            catch(OperationCanceledException) when (token.IsCancellationRequested)
            {
                // normal during shutdown;
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("{context} encountered exception while removing expired blob batches : {exception}", this.traceContext, exception);
            }

            return deletedCount;
        }
    }
}
