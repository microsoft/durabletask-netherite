// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.EventHubsTransport;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    class FasterStorageLayer : IStorageLayer
    {
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly OrchestrationServiceTraceHelper traceHelper;

        readonly ILogger logger;
        readonly ILogger performanceLogger;
        readonly MemoryTracker memoryTracker;

        readonly BlobServiceClient serviceClient;
        readonly BlobContainerClient containerClient;
        readonly BlockBlobClient taskhubParameters;

        public ILoadPublisherService LoadPublisher { get;}

        public long TargetMemorySize { get; set; }

        static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        // the path prefix is used to prevent some issues (races, partial deletions) when recreating a taskhub of the same name
        // since it is a rare circumstance, taking six characters of the Guid is unique enough
        static string TaskhubPathPrefix(TaskhubParameters parameters) => $"{parameters.TaskhubGuid}/";

        public (string containerName, string path) GetTaskhubPathPrefix(TaskhubParameters parameters)
        {
            return (GetContainerName(parameters.TaskhubName), TaskhubPathPrefix(parameters));
        }

        public FasterStorageLayer(NetheriteOrchestrationServiceSettings settings, OrchestrationServiceTraceHelper traceHelper, ILoggerFactory loggerFactory)
        {
            this.settings = settings;
            this.traceHelper = traceHelper;

            this.TestRuntimeAndLoading();
            
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.FasterStorage");
            this.performanceLogger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.FasterStorage.Performance");

            this.memoryTracker = new MemoryTracker((long)(settings.InstanceCacheSizeMB ?? 400) * 1024 * 1024);

            if (settings.TestHooks?.CacheDebugger != null)
            {
                settings.TestHooks.CacheDebugger.MemoryTracker = this.memoryTracker;
            }

            this.serviceClient = settings.BlobStorageConnection.GetAzureStorageV12BlobServiceClient(new Azure.Storage.Blobs.BlobClientOptions());
            string blobContainerName = GetContainerName(settings.HubName);
            this.containerClient = this.serviceClient.GetBlobContainerClient(blobContainerName);
            this.taskhubParameters = this.containerClient.GetBlockBlobClient(settings.TaskhubParametersFilePath);

            this.traceHelper.TraceProgress("Creating LoadPublisher Service");
            if (!string.IsNullOrEmpty(settings.LoadInformationAzureTableName))
            {
                this.LoadPublisher = new AzureTableLoadPublisher(settings.TableStorageConnection, settings.LoadInformationAzureTableName, settings.HubName);
            }
            else
            {
                this.LoadPublisher = new AzureBlobLoadPublisher(settings.BlobStorageConnection, settings.HubName, settings.TaskhubParametersFilePath);
            }
        }

        void TestRuntimeAndLoading()
        {
            // force the loading of potentially problematic dll dependencies here so exceptions are observed early
            var _a = System.Threading.Channels.Channel.CreateBounded<DateTime>(10);
            bool _c = System.Runtime.CompilerServices.Unsafe.AreSame(ref _a, ref _a);

            // throw descriptive exception if run on 32bit platform
            if (!Environment.Is64BitProcess)
            {
                throw new NotSupportedException("Netherite backend requires 64bit, but current process is 32bit.");
            }
        }

        async Task<TaskhubParameters> IStorageLayer.TryLoadTaskhubAsync(bool throwIfNotFound)
        {
            // try load the taskhub parameters
            try
            {
                var downloadResult = await this.taskhubParameters.DownloadContentAsync();
                string blobContents = downloadResult.Value.Content.ToString();
                return JsonConvert.DeserializeObject<TaskhubParameters>(blobContents);
            }
            catch (RequestFailedException ex)
                when (BlobUtilsV12.BlobOrContainerDoesNotExist(ex))
            {
                // container or blob does not exist
                if (throwIfNotFound)
                {
                    throw new NetheriteConfigurationException($"The specified taskhub does not exist (TaskHub={this.settings.HubName}, StorageConnectionName={this.settings.StorageConnectionName}");
                }
                else
                {
                    return null;
                }
            }
        }

        async Task<bool> IStorageLayer.CreateTaskhubIfNotExistsAsync()
        {
            if (this.settings.PartitionManagement == PartitionManagementOptions.RecoveryTester)
            {
                // we do NOT create any resources during recovery testing
                await ((IStorageLayer)this).TryLoadTaskhubAsync(true);
                this.traceHelper.TraceProgress("Confirmed existing taskhub");
                return false;
            }

            Response<Azure.Storage.Blobs.Models.BlobContainerInfo> response = await this.containerClient.CreateIfNotExistsAsync();
            if (response.HasValue)
            {
                this.traceHelper.TraceProgress($"Created new blob container at {this.containerClient.Uri}");
            }
            else
            {
                this.traceHelper.TraceProgress($"Using existing blob container at {this.containerClient.Uri}");
            }


            var taskHubParameters = new TaskhubParameters()
            {
                TaskhubName = this.settings.HubName,
                TaskhubGuid = Guid.NewGuid(),
                CreationTimestamp = DateTime.UtcNow,
                StorageFormat = BlobManager.GetStorageFormat(this.settings),
                PartitionCount = this.settings.PartitionCount,
            };

            // create the load monitor
            await this.LoadPublisher.CreateIfNotExistsAsync(CancellationToken.None);

            // try to create the taskhub blob
            try
            {
                var jsonText = JsonConvert.SerializeObject(
                    taskHubParameters,
                    Newtonsoft.Json.Formatting.Indented,
                    new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });

                await this.taskhubParameters.UploadAsync(
                    content: new MemoryStream(Encoding.UTF8.GetBytes(jsonText)),
                    options: new BlobUploadOptions() { Conditions = new BlobRequestConditions() { IfNoneMatch = ETag.All } },
                    cancellationToken: CancellationToken.None)
                    .ConfigureAwait(false);
                  
                this.traceHelper.TraceProgress("Created new taskhub");

                // zap the partition hub so we start from zero queue positions
                if (this.settings.TransportChoice == TransportChoices.EventHubs)
                {
                    await EventHubsUtil.DeleteEventHubIfExistsAsync(this.settings.EventHubsConnection, EventHubsTransport.PartitionHub, CancellationToken.None);
                }
            }
            catch (RequestFailedException e) when (BlobUtilsV12.BlobAlreadyExists(e))
            {
                // taskhub already exists, possibly because a different node created it faster
                this.traceHelper.TraceProgress("Confirmed existing taskhub");

                return false;
            }

            // we successfully created the taskhub
            return true;
        }

        async Task IStorageLayer.DeleteTaskhubAsync()
        {
            var parameters = await ((IStorageLayer)this).TryLoadTaskhubAsync(throwIfNotFound: false);

            if (parameters != null)
            {
                // delete load information
                await this.LoadPublisher.DeleteIfExistsAsync(CancellationToken.None).ConfigureAwait(false);

                // delete the parameters file which deletes the taskhub logically
                await BlobUtilsV12.ForceDeleteAsync(this.containerClient, this.settings.TaskhubParametersFilePath);

                // delete all the files/blobs in the directory/container that represents this taskhub
                // If this does not complete successfully, some garbage may be left behind.
                await BlobManager.DeleteTaskhubStorageAsync(this.settings, TaskhubPathPrefix(parameters));
            }
        }

        //public static Task DeleteTaskhubStorageAsync(string connectionString, string pageBlobConnectionString, string localFileDirectory, string taskHubName, string pathPrefix)
        //{
        //    var storageAccount = string.IsNullOrEmpty(connectionString) ? null : CloudStorageAccount.Parse(connectionString);
        //    var pageBlobAccount = string.IsNullOrEmpty(pageBlobConnectionString) ? storageAccount : CloudStorageAccount.Parse(pageBlobConnectionString);
        //    return BlobManager.DeleteTaskhubStorageAsync(storageAccount, pageBlobAccount, localFileDirectory, taskHubName, pathPrefix);
        //}

        IPartitionState IStorageLayer.CreatePartitionState(TaskhubParameters parameters)
        {
            return new PartitionStorage(this.settings, TaskhubPathPrefix(parameters), this.memoryTracker, this.logger, this.performanceLogger);
        }

    }
}