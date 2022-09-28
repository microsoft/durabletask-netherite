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

        public Task DeleteIfExistsAsync(CancellationToken cancellationToken)
        {
            // not needed since this blob is stored together with the taskhub storage
            return Task.CompletedTask;
        }

        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            // not needed since this blob is stored together with the taskhub storage
            return Task.CompletedTask;
        }

        public Task PublishAsync(Dictionary<uint, PartitionLoadInfo> info, CancellationToken cancellationToken)
        {
            Task UploadPartitionInfo(uint partitionId, PartitionLoadInfo loadInfo)
            {
                var blobDirectory = this.blobContainer.GetDirectoryReference($"p{partitionId:D2}");
                var blob = blobDirectory.GetBlockBlobReference("loadinfo.json");
                var json = JsonConvert.SerializeObject(loadInfo, Formatting.Indented);
                return blob.UploadTextAsync(json, cancellationToken);
            }

            List<Task> tasks = info.Select(kvp => UploadPartitionInfo(kvp.Key, kvp.Value)).ToList();
            return Task.WhenAll(tasks);           
        }

        public async Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken)
        {
            if (!this.numPartitions.HasValue)
            {
                // determine number of partitions of taskhub
                var blob = this.blobContainer.GetBlockBlobReference("taskhubparameters.json");
                var jsonText = await blob.DownloadTextAsync().ConfigureAwait(false);

/* Unmerged change from project 'DurableTask.Netherite (netcoreapp3.1)'
Before:
                var info = JsonConvert.DeserializeObject<EventHubs.TaskhubParameters>(jsonText);
After:
                var info = JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
*/
                var info = JsonConvert.DeserializeObject<Netherite.Abstractions.TaskhubParameters>(jsonText);
                this.numPartitions = info.PartitionCount;
            }

            async Task<(uint,PartitionLoadInfo)> DownloadPartitionInfo(uint partitionId)
            {
                var blobDirectory = this.blobContainer.GetDirectoryReference($"p{partitionId:D2}");
                var blob = blobDirectory.GetBlockBlobReference("loadinfo.json");
                string json = await blob.DownloadTextAsync(cancellationToken);
                PartitionLoadInfo info = JsonConvert.DeserializeObject<PartitionLoadInfo>(json);
                return (partitionId, info);
            }

            var tasks = Enumerable.Range(0, this.numPartitions.Value).Select(partitionId => DownloadPartitionInfo((uint) partitionId)).ToList();
            await Task.WhenAll(tasks);
            return tasks.Select(task => task.Result).ToDictionary(pair => pair.Item1, pair => pair.Item2);
        }
    }
}
