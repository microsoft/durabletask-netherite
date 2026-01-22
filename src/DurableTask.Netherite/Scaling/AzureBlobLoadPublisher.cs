// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Faster;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class AzureBlobLoadPublisher : ILoadPublisherService
    {
        readonly string taskHubName;
        readonly BlobContainerClient blobContainerClient;
        readonly string taskhubParametersFilePath;      
        TaskhubParameters parameters;
        static readonly BlobUploadOptions BlobUploadOptions = new() { HttpHeaders = new BlobHttpHeaders() { ContentType = "application/json" } };

        readonly static JsonSerializerSettings serializerSettings = new JsonSerializerSettings() 
        {
            TypeNameHandling = TypeNameHandling.None,
            MissingMemberHandling = MissingMemberHandling.Ignore,
        };

        public AzureBlobLoadPublisher(ConnectionInfo connectionInfo, string taskHubName, string taskHubParametersFilePath)
        {
            this.blobContainerClient = this.GetBlobContainer(connectionInfo, taskHubName);
            this.taskHubName = taskHubName;
            this.taskhubParametersFilePath = taskHubParametersFilePath;
        }

        BlobContainerClient GetBlobContainer(ConnectionInfo connectionInfo, string taskHubName)
        {
            BlobServiceClient serviceClient = connectionInfo.GetAzureStorageV12BlobServiceClient(new Azure.Storage.Blobs.BlobClientOptions());
            string containerName = BlobManager.GetContainerName(taskHubName);
            return serviceClient.GetBlobContainerClient(containerName);
        }

        public TimeSpan PublishInterval => TimeSpan.FromSeconds(10);

        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            // not needed since the blobs are stored in the taskhub's container
            return Task.CompletedTask;
        }

        async ValueTask<bool> LoadParameters(bool throwIfNotFound, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (this.parameters == null)
            {
                this.parameters = await ReadJsonBlobAsync<Netherite.Abstractions.TaskhubParameters>(
                    this.blobContainerClient.GetBlockBlobClient(this.taskhubParametersFilePath),
                    throwIfNotFound: throwIfNotFound,
                    throwOnParseError: throwIfNotFound,
                    cancellationToken).ConfigureAwait(false);
            }
            return this.parameters != null;
        }

        static async Task<T> ReadJsonBlobAsync<T>(BlockBlobClient blobClient, bool throwIfNotFound, bool throwOnParseError, CancellationToken token) where T : class
        {
            try
            {
                var downloadResult = await blobClient.DownloadContentAsync();
                string blobContents = downloadResult.Value.Content.ToString();
                return JsonConvert.DeserializeObject<T>(blobContents);
            }
            catch (RequestFailedException ex)
                when (BlobUtilsV12.BlobOrContainerDoesNotExist(ex) && !throwIfNotFound)
            {
                // container or blob does not exist
            }
            catch (JsonException) when (!throwOnParseError)
            {
                // cannot parse content of blob
            }

            return null;
        }

        public async Task PublishAsync(Dictionary<uint, PartitionLoadInfo> info, CancellationToken cancellationToken)
        {
            await this.LoadParameters(throwIfNotFound: true, cancellationToken).ConfigureAwait(false);
         
            async Task UploadPartitionInfo(uint partitionId, PartitionLoadInfo loadInfo)
            {
                var blobClient = this.blobContainerClient.GetBlockBlobClient($"{this.parameters.TaskhubGuid}/p{partitionId:D2}/loadinfo.json");
                var json = JsonConvert.SerializeObject(loadInfo, Formatting.Indented, serializerSettings);
                await blobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), BlobUploadOptions, cancellationToken).ConfigureAwait(false);
            }

            List<Task> tasks = info.Select(kvp => UploadPartitionInfo(kvp.Key, kvp.Value)).ToList();
            await Task.WhenAll(tasks);
        }

        public async Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken)
        {
            await this.LoadParameters(throwIfNotFound: true, cancellationToken).ConfigureAwait(false);

            async Task<(uint, PartitionLoadInfo)> DownloadPartitionInfo(uint partitionId)
            {
                PartitionLoadInfo info = await ReadJsonBlobAsync<PartitionLoadInfo>(
                    this.blobContainerClient.GetBlockBlobClient($"{this.parameters.TaskhubGuid}/p{partitionId:D2}/loadinfo.json"), 
                    throwIfNotFound: false, 
                    throwOnParseError: true,
                    cancellationToken).ConfigureAwait(false);
                return (partitionId, info);
            }

            var tasks = Enumerable.Range(0, this.parameters.PartitionCount).Select(partitionId => DownloadPartitionInfo((uint)partitionId)).ToList();
            await Task.WhenAll(tasks).ConfigureAwait(false);
            return tasks.Select(task => task.Result).Where(pair => pair.Item2 != null).ToDictionary(pair => pair.Item1, pair => pair.Item2);
        }

        public async Task DeleteIfExistsAsync(CancellationToken cancellationToken)
        {
            if (await this.LoadParameters(throwIfNotFound: false, cancellationToken).ConfigureAwait(false))
            {
                var tasks = Enumerable.Range(0, this.parameters.PartitionCount).Select(partitionId => 
                    BlobUtilsV12.ForceDeleteAsync(this.blobContainerClient, $"{this.parameters.TaskhubGuid}/p{partitionId:D2}/loadinfo.json")).ToList();
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }
    }
}
