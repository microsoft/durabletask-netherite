// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.EventHubsTransport;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.SystemFunctions;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    class FasterStorageLayer : IStorageLayer
    {
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly OrchestrationServiceTraceHelper traceHelper;

        readonly CloudStorageAccount storageAccount;
        readonly string localFileDirectory;
        readonly CloudStorageAccount pageBlobStorageAccount;

        readonly ILogger logger;
        readonly ILogger performanceLogger;
        readonly MemoryTracker memoryTracker;
        readonly CloudBlobContainer cloudBlobContainer;
        readonly CloudBlockBlob taskhubParameters;

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

            string connectionString = settings.ResolvedStorageConnectionString;
            string pageBlobConnectionString = settings.ResolvedPageBlobStorageConnectionString;

            this.TestRuntimeAndLoading();

            if (!string.IsNullOrEmpty(settings.UseLocalDirectoryForPartitionStorage))
            {
                this.localFileDirectory = settings.UseLocalDirectoryForPartitionStorage;
            }
            else
            {
                this.storageAccount = CloudStorageAccount.Parse(connectionString);
            }
            if (pageBlobConnectionString != connectionString && !string.IsNullOrEmpty(pageBlobConnectionString))
            {
                this.pageBlobStorageAccount = CloudStorageAccount.Parse(pageBlobConnectionString);
            }
            else
            {
                this.pageBlobStorageAccount = this.storageAccount;
            }
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.FasterStorage");
            this.performanceLogger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.FasterStorage.Performance");

            this.memoryTracker = new MemoryTracker((long)(settings.InstanceCacheSizeMB ?? 400) * 1024 * 1024);

            if (settings.TestHooks?.CacheDebugger != null)
            {
                settings.TestHooks.CacheDebugger.MemoryTracker = this.memoryTracker;
            }

            var blobContainerName = GetContainerName(settings.HubName);
            var cloudBlobClient = this.storageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskhubParameters = this.cloudBlobContainer.GetBlockBlobReference("taskhubparameters.json");

            this.traceHelper.TraceProgress("Creating LoadMonitor Service");
            if (!string.IsNullOrEmpty(settings.LoadInformationAzureTableName))
            {
                this.LoadPublisher = new AzureTableLoadPublisher(settings.ResolvedStorageConnectionString, settings.LoadInformationAzureTableName, settings.HubName);
            }
            else
            {
                this.LoadPublisher = new AzureBlobLoadPublisher(settings.ResolvedStorageConnectionString, settings.HubName);
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
                var jsonText = await this.taskhubParameters.DownloadTextAsync();
                return JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == (int)System.Net.HttpStatusCode.NotFound)
            {
                if (throwIfNotFound)
                {
                    throw new InvalidOperationException($"The specified taskhub does not exist (TaskHub={this.settings.HubName}, StorageConnectionName={this.settings.StorageConnectionName}");
                }
                else
                {
                    return null;
                }
            }
        }       

        async Task<bool> IStorageLayer.CreateTaskhubIfNotExistsAsync()
        {
            bool containerCreated = await this.cloudBlobContainer.CreateIfNotExistsAsync();
            if (containerCreated)
            {
                this.traceHelper.TraceProgress($"Created new blob container at {this.cloudBlobContainer.Uri}");
            }
            else
            {
                this.traceHelper.TraceProgress($"Using existing blob container at {this.cloudBlobContainer.Uri}");
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

                var noOverwrite = AccessCondition.GenerateIfNoneMatchCondition("*");
                await this.taskhubParameters.UploadTextAsync(jsonText, null, noOverwrite, null, null);
                this.traceHelper.TraceProgress("Created new taskhub");

                // zap the partition hub so we start from zero queue positions
                if (!TransportConnectionString.IsPseudoConnectionString(this.settings.ResolvedTransportConnectionString))
                {
                    await EventHubsUtil.DeleteEventHubIfExistsAsync(this.settings.ResolvedTransportConnectionString, EventHubsTransport.PartitionHub);
                }
            }
            catch (StorageException e) when (BlobUtils.BlobAlreadyExists(e))
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
                // first, delete the parameters file which deletes the taskhub logically
                await BlobUtils.ForceDeleteAsync(this.taskhubParameters);

                // delete load information
                await this.LoadPublisher.DeleteIfExistsAsync(CancellationToken.None).ConfigureAwait(false);

                // delete all the files/blobs in the directory/container that represents this taskhub
                // If this does not complete successfully, some garbage may be left behind.
                await BlobManager.DeleteTaskhubStorageAsync(this.storageAccount, this.pageBlobStorageAccount, this.localFileDirectory, parameters.TaskhubName, TaskhubPathPrefix(parameters));
            }
        }

        public static Task DeleteTaskhubStorageAsync(string connectionString, string pageBlobConnectionString, string localFileDirectory, string taskHubName, string pathPrefix)
        {
            var storageAccount = string.IsNullOrEmpty(connectionString) ? null : CloudStorageAccount.Parse(connectionString);
            var pageBlobAccount = string.IsNullOrEmpty(pageBlobConnectionString) ? storageAccount : CloudStorageAccount.Parse(pageBlobConnectionString);
            return BlobManager.DeleteTaskhubStorageAsync(storageAccount, pageBlobAccount, localFileDirectory, taskHubName, pathPrefix);
        }

        IPartitionState IStorageLayer.CreatePartitionState(TaskhubParameters parameters)
        {
            return new PartitionStorage(this.settings, TaskhubPathPrefix(parameters), this.memoryTracker, this.logger, this.performanceLogger);
        }

    }
}