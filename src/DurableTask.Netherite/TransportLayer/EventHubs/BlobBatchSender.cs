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
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Faster;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

    class BlobBatchSender
    {
        readonly string traceContext;
        readonly EventHubsTraceHelper traceHelper;
        readonly EventHubsTraceHelper lowestTraceLevel;
        readonly BlobContainerClient containerClient;
        readonly Random random = new Random();
        readonly BlobUploadOptions options;

        public const string PathPrefix = "eh-batches/";

        public BlobBatchSender(string traceContext, EventHubsTraceHelper traceHelper, NetheriteOrchestrationServiceSettings settings)
        {
            this.traceContext = traceContext;
            this.traceHelper = traceHelper;
            this.lowestTraceLevel = traceHelper.IsEnabled(LogLevel.Trace) ? traceHelper : null;
            var serviceClient = BlobUtilsV12.GetServiceClients(settings.BlobStorageConnection).WithRetries;
            string containerName = BlobManager.GetContainerName(settings.HubName);
            this.containerClient = serviceClient.GetBlobContainerClient(containerName);
            this.options = new BlobUploadOptions() {  };
        }

        // these constants influence the max size of batches transmitted in EH and via blobs.
        // note that these cannot be made arbitrarily large (EH batches cannot exceed max batch size on EH, and neither can the indexes of blob batches)
        public int MaxEventHubsBatchBytes = 30 * 1024;
        public int MaxEventHubsBatchEvents = 300;
        public int MaxBlobBatchBytes = 500 * 1024;
        public int MaxBlobBatchEvents = 5000;

        string GetRandomBlobName()
        {
            uint random = (uint)this.random.Next();
            return $"{DateTime.UtcNow:o}-{random:X8}";
        }

        public async Task<EventData> UploadEventsAsync(MemoryStream stream, List<int> packetOffsets, byte[] taskHubGuid, CancellationToken token)
        {
            string blobName = this.GetRandomBlobName();
            string blobPath = $"{PathPrefix}{blobName}";
            BlockBlobClient blobClient = this.containerClient.GetBlockBlobClient(blobPath);

            long totalBytes = stream.Position;
            stream.Seek(0, SeekOrigin.Begin);
            stream.SetLength(totalBytes);

            this.lowestTraceLevel?.LogTrace("{context} is writing blob {blobName} ({size} bytes)", this.traceContext, blobClient.Name, totalBytes);

            await BlobManager.AsynchronousStorageWriteMaxConcurrency.WaitAsync();

            try
            {
                await blobClient.UploadAsync(stream, this.options, token).ConfigureAwait(false);

                this.lowestTraceLevel?.LogTrace("{context} wrote blob {blobName}", this.traceContext, blobClient.Name);

                // create a message to send via event hubs
                stream.SetLength(0);
                Packet.Serialize(blobName, packetOffsets, stream, taskHubGuid);
                var arraySegment = new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position);
                var eventData = new EventData(arraySegment);
                return eventData;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // normal during shutdown
                throw;
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("{context} failed to write blob {blobName} : {exception}", this.traceContext, blobClient.Name, exception);
                throw;
            }
            finally
            {
                BlobManager.AsynchronousStorageWriteMaxConcurrency.Release();
            }
        }
    }
}
