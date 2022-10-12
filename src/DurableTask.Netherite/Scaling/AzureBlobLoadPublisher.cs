// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using DurableTask.Netherite.Faster;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class AzureBlobLoadPublisher : ILoadPublisherService
    {
        readonly string taskHubName;
        readonly CloudBlobContainer blobContainer;

        readonly static JsonSerializerSettings serializerSettings = new JsonSerializerSettings() 
        {
            TypeNameHandling = TypeNameHandling.None,
            MissingMemberHandling = MissingMemberHandling.Ignore,
        };

        int? numPartitions;

        public AzureBlobLoadPublisher(string connectionString, string taskHubName)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = cloudStorageAccount.CreateCloudBlobClient();
            string containerName = BlobManager.GetContainerName(taskHubName);
            this.blobContainer = serviceClient.GetContainerReference(containerName);
            this.taskHubName = taskHubName;
        }

        public TimeSpan PublishInterval => TimeSpan.FromSeconds(10);

        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            // not needed since the blobs are stored in the taskhub's container
            return Task.CompletedTask;
        }

        public Task PublishAsync(Dictionary<uint, PartitionLoadInfo> info, CancellationToken cancellationToken)
        {
            Task UploadPartitionInfo(uint partitionId, PartitionLoadInfo loadInfo)
            {
                var blobDirectory = this.blobContainer.GetDirectoryReference($"p{partitionId:D2}");
                var blob = blobDirectory.GetBlockBlobReference("loadinfo.json");
                var json = JsonConvert.SerializeObject(loadInfo, Formatting.Indented, serializerSettings);
                return blob.UploadTextAsync(json, cancellationToken);
            }

            List<Task> tasks = info.Select(kvp => UploadPartitionInfo(kvp.Key, kvp.Value)).ToList();
            return Task.WhenAll(tasks);
        }

        public async Task<T> ReadJsonBlobAsync<T>(CloudBlockBlob blob, bool throwIfNotFound, bool throwOnParseError, CancellationToken token) where T : class
        {
            try
            {
                var jsonText = await blob.DownloadTextAsync(token).ConfigureAwait(false);
                return JsonConvert.DeserializeObject<T>(jsonText);
            }
            catch (StorageException e) when (!throwIfNotFound && e.RequestInformation?.HttpStatusCode == 404)
            {
                // container or blob does not exist
            }
            catch (JsonException) when (!throwOnParseError)
            {
                // cannot parse content of blob
            }
            catch(StorageException e) when (e.InnerException is OperationCanceledException operationCanceledException)
            {
                throw new OperationCanceledException("Blob read was canceled.", operationCanceledException);
            }

            return null;
        }

        public async Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken)
        {
            if (!this.numPartitions.HasValue)
            {
                // determine number of partitions of taskhub
                var info = await this.ReadJsonBlobAsync<Netherite.Abstractions.TaskhubParameters>(
                    this.blobContainer.GetBlockBlobReference("taskhubparameters.json"),
                    throwIfNotFound: true,
                    throwOnParseError: true,
                    cancellationToken).ConfigureAwait(false);

                this.numPartitions = info.PartitionCount;
            }

            async Task<(uint, PartitionLoadInfo)> DownloadPartitionInfo(uint partitionId)
            {
                PartitionLoadInfo info = await this.ReadJsonBlobAsync<PartitionLoadInfo>(
                    this.blobContainer.GetDirectoryReference($"p{partitionId:D2}").GetBlockBlobReference("loadinfo.json"), 
                    throwIfNotFound: false, 
                    throwOnParseError: true,
                    cancellationToken).ConfigureAwait(false);
                return (partitionId, info);
            }

            var tasks = Enumerable.Range(0, this.numPartitions.Value).Select(partitionId => DownloadPartitionInfo((uint)partitionId)).ToList();
            await Task.WhenAll(tasks).ConfigureAwait(false);
            return tasks.Select(task => task.Result).Where(pair => pair.Item2 != null).ToDictionary(pair => pair.Item1, pair => pair.Item2);
        }

        public async Task DeleteIfExistsAsync(CancellationToken cancellationToken)
        {
            if (!this.numPartitions.HasValue)
            {
                // determine number of partitions of taskhub
                var info = await this.ReadJsonBlobAsync<Netherite.Abstractions.TaskhubParameters>(
                    this.blobContainer.GetBlockBlobReference("taskhubparameters.json"),
                    throwIfNotFound: false,
                    throwOnParseError: false,
                    cancellationToken).ConfigureAwait(false);

                if (info == null)
                {
                    return;
                }
                else
                {
                    this.numPartitions = info.PartitionCount;
                }
            }

            async Task DeletePartitionInfo(uint partitionId)
            {
                var blob = this.blobContainer.GetDirectoryReference($"p{partitionId:D2}").GetBlockBlobReference("loadinfo.json");
                await BlobUtils.ForceDeleteAsync(blob).ConfigureAwait(false);
            }

            var tasks = Enumerable.Range(0, this.numPartitions.Value).Select(partitionId => DeletePartitionInfo((uint)partitionId)).ToList();
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }
}
