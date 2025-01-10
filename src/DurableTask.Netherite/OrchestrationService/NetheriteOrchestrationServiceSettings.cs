// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime;
    using DurableTask.Core;
    using FASTER.core;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    /// <summary>
    /// Settings for the <see cref="NetheriteOrchestrationService"/> class.
    /// </summary>
    [JsonObject]
    public class NetheriteOrchestrationServiceSettings
    {
        /// <summary>
        /// The name of the taskhub.
        /// Matches corresponding property in Microsoft.Azure.WebJobs.Extensions.DurableTask.DurableTaskOptions.
        /// </summary>
        public string HubName { get; set; }

        /// <summary>
        /// Gets or sets the name for resolving the Azure storage connection string.
        /// </summary>
        public string StorageConnectionName { get; set; } = "AzureWebJobsStorage";

        /// <summary>
        /// Gets or sets the name for resolving the Eventhubs namespace connection string.
        /// Pseudo-connection-strings "Memory" or "SingleHost" can be used to configure
        /// in-memory emulation, or single-host configuration, respectively.
        /// </summary>
        public string EventHubsConnectionName { get; set; } = "EventHubsConnection";

        /// <summary>
        /// Gets or sets the identifier for the current worker.
        /// </summary>
        public string WorkerId { get; set; } = Environment.MachineName;

        /// <summary>
        /// Gets or sets the number of partitions to use when creating a new taskhub. 
        /// If a taskhub already exists, this number is irrelevant.
        /// </summary>
        public int PartitionCount { get; set; } = 12;

        /// <summary>
        /// Optionally, a name for an Azure Table to use for publishing load information. If set to null or empty,
        /// then Azure blobs are used instead. The use of Azure blobs is currently not supported on consumption plans, or on elastic premium plans without runtime scaling.
        /// </summary>
        public string LoadInformationAzureTableName { get; set; } = "DurableTaskPartitions";

        /// <summary>
        /// Tuning parameters for the FASTER logs
        /// </summary>
        public Faster.BlobManager.FasterTuningParameters FasterTuningParameters { get; set; } = null;

        /// <summary>
        /// Gets or sets the maximum number of activity work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// Matches corresponding property in Microsoft.Azure.WebJobs.Extensions.DurableTask.DurableTaskOptions.
        /// </summary>
        public int MaxConcurrentActivityFunctions { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of orchestration work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// Matches corresponding property in Microsoft.Azure.WebJobs.Extensions.DurableTask.DurableTaskOptions.
        /// </summary>
        public int MaxConcurrentOrchestratorFunctions { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of entity work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// Matches corresponding property in Microsoft.Azure.WebJobs.Extensions.DurableTask.DurableTaskOptions.
        /// </summary>
        public int MaxConcurrentEntityFunctions { get; set; } = 100;

        /// <summary>
        /// Whether to use separate work item queues for entities and orchestrators.
        /// This defaults to false, to maintain compatility with legacy front ends.
        /// Newer front ends explicitly set this to true.
        /// </summary>
        public bool UseSeparateQueueForEntityWorkItems { get; set; } = false;

        /// <summary>
        /// Gets or sets the maximum number of entity operations that are processed as a single batch.
        /// The default value is 1000.
        /// Matches corresponding property in Microsoft.Azure.WebJobs.Extensions.DurableTask.DurableTaskOptions.
        /// </summary>
        public int MaxEntityOperationBatchSize { get; set; } = 1000;

        /// <summary>
        /// Gets or sets the number of dispatchers used to dispatch orchestrations.
        /// </summary>
        public int OrchestrationDispatcherCount { get; set; } = 1;

        /// <summary>
        /// Gets or sets the number of dispatchers used to dispatch activities.
        /// </summary>
        public int ActivityDispatcherCount { get; set; } = 1;

        /// <summary>
        /// Limit for how much memory on each node should be used for caching instance states and histories
        /// </summary>
        public int? InstanceCacheSizeMB { get; set; } = null;

        /// <summary>
        /// Gets or sets the partition management option
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public PartitionManagementOptions PartitionManagement { get; set; } = PartitionManagementOptions.EventProcessorHost;

        /// <summary>
        /// Additional parameters for the partition management, if necessary
        /// </summary>
        public string PartitionManagementParameters { get; set; } = null;

        /// <summary>
        /// The path to the file containing the taskhub parameters.
        /// </summary>
        public string TaskhubParametersFilePath { get; set; } = "taskhubparameters.json";

        /// <summary>
        /// Gets or sets the activity scheduler option
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public ActivitySchedulerOptions ActivityScheduler { get; set; } = ActivitySchedulerOptions.Locavore;

        /// <summary>
        /// Gets or sets a flag indicating whether to enable caching of execution cursors to avoid replay.
        /// </summary>
        public bool CacheOrchestrationCursors { get; set; } = true;

        /// <summary>
        /// Whether we should carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew.
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew { get; set; } = BehaviorOnContinueAsNew.Carryover;

        /// <summary>
        /// When true, will throw an exception when attempting to create an orchestration with an existing dedupe status.
        /// </summary>
        public bool ThrowExceptionOnInvalidDedupeStatus { get; set; } = false;

        /// <summary>
        /// Whether to checkpoint the current state of a partition when it is stopped. This improves recovery time but
        /// lengthens shutdown time and can cause memory pressure if many partitions are stopped at the same time,
        /// for example if a host is shutting down.
        /// </summary>
        public bool TakeStateCheckpointWhenStoppingPartition { get; set; } = false;

        /// <summary>
        /// A limit on how many bytes to append to the log before initiating a state checkpoint. The default is 20MB.
        /// </summary>
        public long MaxNumberBytesBetweenCheckpoints { get; set; } = 20 * 1024 * 1024;

        /// <summary>
        /// A limit on how many events to append to the log before initiating a state checkpoint. The default is 10000.
        /// </summary>
        public long MaxNumberEventsBetweenCheckpoints { get; set; } = 10 * 1000;

        /// <summary>
        /// A limit on how long to wait between state checkpoints, in milliseconds. The default is 60s.
        /// </summary>
        public long IdleCheckpointFrequencyMs { get; set; } = 60 * 1000;

        /// <summary>
        /// Set this to a local file path to make FASTER use local files instead of blobs. Currently,
        /// this makes sense only for local testing and debugging.
        /// </summary>
        public string UseLocalDirectoryForPartitionStorage { get; set; } = null;

        /// <summary>
        /// Whether to keep an in-memory set of all instance ids in memory. This is required for supporting paged queries.
        /// </summary>
        public bool KeepInstanceIdsInMemory = true;

        /// <summary>
        /// Whether to immediately shut down the transport layer and terminate the process when a fatal exception is observed.
        /// This is true by default, to enable failing hosts to leave quickly which allows other hosts to recover the partitions more quickly.
        /// </summary>
        public bool EmergencyShutdownOnFatalExceptions = true;

        /// <summary>
        /// Forces steps to pe persisted before applying their effects, disabling all pipelining.
        /// </summary>
        public bool PersistStepsFirst { get; set; } = false;

        /// <summary>
        /// If true, the start of work items is delayed until the dequeue count
        /// is persisted. Defaults to false, which improves latency but means the 
        /// reported dequeue count may be lower than the actual dequeue count in some cases.
        /// </summary>
        public bool PersistDequeueCountBeforeStartingWorkItem { get; set; } = false;

        /// <summary>
        /// Pack TaskMessages generated by a single work item for the same destination into a single event.
        /// </summary>
        public int PackPartitionTaskMessages { get; set; } = 100;

        /// <summary>
        /// Time limit for partition startup, in minutes.
        /// </summary>
        public int PartitionStartupTimeoutMinutes { get; set; } = 15;

        /// <summary>
        /// If true, disables the prefetching during replay.
        /// </summary>
        public bool DisablePrefetchDuringReplay { get; set; } = false;

        /// <summary>
        /// Allows attaching additional checkers and debuggers during testing.
        /// </summary>
        [JsonIgnore]
        public TestHooks TestHooks { get; set; } = null;

        /// <summary>
        /// A lower limit on the severity level of trace events emitted by the transport layer.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel TransportLogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// A lower limit on the severity level of trace events emitted by the storage layer.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel StorageLogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// A lower limit on the severity level of event processor trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel EventLogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// A lower limit on the severity level of work item trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel WorkItemLogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// A lower limit on the severity level of client trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel ClientLogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// A lower limit on the severity level of load monitor trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel LoadMonitorLogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// A lower limit on the severity level of all other trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel LogLevelLimit { get; set; } = LogLevel.Debug;

        #region Compatibility Shim

        /// <summary>
        /// The resolved storage connection string. Is never serialized or deserialized.
        /// </summary>
        [JsonIgnore]
        [Obsolete("connections should be resolved by calling settings.Validate(ConnectionResolver resolver)")]
        public string ResolvedStorageConnectionString { get; set; }

        /// <summary>
        /// The resolved event hubs connection string. Is never serialized or deserialized.
        /// </summary>
        [JsonIgnore]
        [Obsolete("connections should be resolved by calling settings.Validate(ConnectionResolver resolver)")]
        public string ResolvedTransportConnectionString { get; set; }

        /// <summary>
        /// A name for resolving a storage connection string to be used specifically for the page blobs, or null if page blobs are to be stored in the default account.
        /// </summary>
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        [Obsolete("connections should be resolved by calling settings.Validate(ConnectionResolver resolver)")]
        public string PageBlobStorageConnectionName { get; set; } = null;

        /// <summary>
        /// The resolved page blob storage connection string, or null if page blobs are to be stored in the default account. Is never serialized or deserialized.
        /// </summary>
        [JsonIgnore]
        [Obsolete("connections should be resolved by calling settings.Validate(ConnectionResolver resolver)")]
        public string ResolvedPageBlobStorageConnectionString { get; set; }

        class CompatibilityResolver : ConnectionResolver
        {
            readonly Func<string, string> nameResolver;

            CompatibilityResolver(Func<string, string> nameResolver)
            {
                this.nameResolver = nameResolver;
            }

            public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType)
            {
                throw new NotImplementedException();
            }

            public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
            {
                throw new NotImplementedException();
            }
        }

        #endregion

        #region Parameters that are set during resolution

        /// <summary>
        /// The type of storage layer to be used
        /// </summary>
        [JsonIgnore]
        public StorageChoices StorageChoice { get; protected set; }

        /// <summary>
        /// The type of transport layer to be used
        /// </summary>
        [JsonIgnore]
        public TransportChoices TransportChoice { get; protected set; }

        /// <summary>
        /// The connection information for Azure Storage blobs.
        /// If not explicitly set, this is populated during validation by resolving <see cref="StorageConnectionName"/>.
        /// </summary>
        [JsonIgnore]
        public ConnectionInfo BlobStorageConnection { get; protected set; }

        /// <summary>
        /// The connection information for Azure Storage tables.
        /// If not explicitly set, this is populated during validation by resolving <see cref="StorageConnectionName"/>.
        /// </summary>
        [JsonIgnore]
        public ConnectionInfo TableStorageConnection { get; protected set; }

        /// <summary>
        /// The connection information for the event hubs namespace.
        /// If not explicitly set, this is populated during validation by resolving <see cref="EventHubsConnectionName"/>.
        /// </summary>
        [JsonIgnore]
        public ConnectionInfo EventHubsConnection { get; protected set; }

        /// <summary>
        /// The connection information for Azure Storage page blobs. 
        ///This is usually null, which means the same <see cref="BlobStorageConnection"/> should be used for page blobs also.
        /// </summary>
        [JsonIgnore]
        public ConnectionInfo PageBlobStorageConnection { get; protected set; }

        /// <summary>
        /// Whether the storage layer was configured to use a different connection for page blobs than for other blobs
        /// </summary>
        [JsonIgnore]
        internal bool UseSeparatePageBlobStorage => this.PageBlobStorageConnection != null;

        [JsonIgnore]
        public string StorageAccountName
            => this.StorageChoice == StorageChoices.Memory ? "Memory" : this.BlobStorageConnection.ResourceName;

        /// <summary>
        /// Whether the settings have been validated and resolved
        /// </summary>
        [JsonIgnore]
        public bool ResolutionComplete { get; protected set; }

        #endregion

        /// <summary>
        /// Validates the settings, throwing exceptions if there are issues.
        /// </summary>
        /// <param name="nameResolver">Optionally, a resolver for connection names.</param>
        public void Validate(Func<string, string> connectionNameToConnectionString = null)
        {
            this.Validate(new CompatibilityConnectionResolver(this, connectionNameToConnectionString));
        }

        /// <summary>
        /// Validates the settings and resolves the connections, throwing exceptions if there are issues.
        /// </summary>
        /// <param name="resolver">A connection resolver.</param>
        public void Validate(ConnectionResolver resolver)
        {
            if (string.IsNullOrEmpty(this.HubName))
            {
                throw new NetheriteConfigurationException($"Must specify {nameof(this.HubName)} for Netherite storage provider.");
            }

            ValidateTaskhubName(this.HubName);

            if (this.PartitionCount < 1 || this.PartitionCount > 32)
            {
                throw new ArgumentOutOfRangeException(nameof(this.PartitionCount));
            }

            if (this.MaxConcurrentOrchestratorFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(this.MaxConcurrentOrchestratorFunctions));
            }

            if (this.MaxConcurrentActivityFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(this.MaxConcurrentActivityFunctions));
            }

            resolver.ResolveLayerConfiguration(this.EventHubsConnectionName, out var storage, out var transport);
            this.StorageChoice = storage;
            this.TransportChoice = transport;

            if (this.TransportChoice == TransportChoices.EventHubs)
            {
                // we need a valid event hubs connection

                if (string.IsNullOrEmpty(this.EventHubsConnectionName) && resolver is ConnectionNameToConnectionStringResolver)
                {
                    throw new NetheriteConfigurationException($"Must specify {nameof(this.EventHubsConnectionName)} for Netherite storage provider.");
                }
                try
                {
                    this.EventHubsConnection = resolver.ResolveConnectionInfo(this.HubName, this.EventHubsConnectionName, ConnectionResolver.ResourceType.EventHubsNamespace);
                }
                catch (Exception e)
                {
                    throw new NetheriteConfigurationException($"Could not resolve {nameof(this.EventHubsConnectionName)}={this.EventHubsConnectionName} to create an eventhubs connection for Netherite storage provider: {e.Message}", e);
                }
                if (this.EventHubsConnection == null)
                {
                    throw new NetheriteConfigurationException($"Could not resolve {nameof(this.EventHubsConnectionName)}={this.EventHubsConnectionName} to create an eventhubs connection for Netherite storage provider.");
                }
            }

            if (this.StorageChoice == StorageChoices.Faster || this.TransportChoice == TransportChoices.EventHubs)
            {
                // we need a valid blob storage connection

                if (string.IsNullOrEmpty(this.StorageConnectionName) && resolver is ConnectionNameToConnectionStringResolver)
                {
                    throw new NetheriteConfigurationException($"Must specify {nameof(this.StorageConnectionName)} for Netherite storage provider.");
                }
                try
                {
                    this.BlobStorageConnection = resolver.ResolveConnectionInfo(this.HubName, this.StorageConnectionName, ConnectionResolver.ResourceType.BlobStorage);
                }
                catch (Exception e)
                {
                    throw new NetheriteConfigurationException($"Could not resolve {nameof(this.StorageConnectionName)}={this.StorageConnectionName} to create a blob storage connection for Netherite storage provider: {e.Message}", e);
                }
                if (this.BlobStorageConnection == null)
                {
                    throw new NetheriteConfigurationException($"Could not resolve {nameof(this.StorageConnectionName)}={this.StorageConnectionName} to create a blob storage connection for Netherite storage provider.");
                }
            }

            if (this.StorageChoice == StorageChoices.Faster && !string.IsNullOrEmpty(this.LoadInformationAzureTableName))
            {
                // we need a valid table storage connection
                try
                {
                    this.TableStorageConnection = resolver.ResolveConnectionInfo(this.HubName, this.StorageConnectionName, ConnectionResolver.ResourceType.TableStorage);
                }
                catch (Exception e)
                {
                    throw new NetheriteConfigurationException($"Could not resolve {nameof(this.StorageConnectionName)}={this.StorageConnectionName} to create a table storage connection for Netherite storage provider: {e.Message}", e);
                }
                if (this.TableStorageConnection == null)
                {
                    throw new NetheriteConfigurationException($"Could not resolve {nameof(this.StorageConnectionName)}={this.StorageConnectionName} to create a table storage connection for Netherite storage provider.");
                }
            }

            if (this.StorageChoice == StorageChoices.Faster)
            {
                // some custom resolvers may specify a separate page blob connection, but usually this will be null
                this.PageBlobStorageConnection = resolver.ResolveConnectionInfo(this.HubName, this.StorageConnectionName, ConnectionResolver.ResourceType.PageBlobStorage);
            }

            // we have completed validation and resolution
            this.ResolutionComplete = true;
        }

        const int MinTaskHubNameSize = 3;
        const int MaxTaskHubNameSize = 45;

        public static void ValidateTaskhubName(string taskhubName)
        {
            if (taskhubName.Length < MinTaskHubNameSize || taskhubName.Length > MaxTaskHubNameSize)
            {
                throw new NetheriteConfigurationException(GetTaskHubErrorString(taskhubName));
            }

            try
            {
                BlobUtilsV11.ValidateContainerName(taskhubName.ToLowerInvariant());
                BlobUtilsV11.ValidateBlobName(taskhubName);
            }
            catch (ArgumentException e)
            {
                throw new NetheriteConfigurationException(GetTaskHubErrorString(taskhubName), e);
            }
        }

        static string GetTaskHubErrorString(string hubName)
        {
            return $"Task hub name '{hubName}' should contain only alphanumeric characters, start with a letter, and have length between {MinTaskHubNameSize} and {MaxTaskHubNameSize}.";
        }
    }
}