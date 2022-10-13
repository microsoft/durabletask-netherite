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
        /// The name of the taskhub. Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
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
        /// then Azure blobs are used instead.
        /// </summary>
        public string LoadInformationAzureTableName { get; set; } = "DurableTaskPartitions";

        /// <summary>
        /// Tuning parameters for the FASTER logs
        /// </summary>
        public Faster.BlobManager.FasterTuningParameters FasterTuningParameters { get; set; } = null;

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public int MaxConcurrentActivityFunctions { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public int MaxConcurrentOrchestratorFunctions { get; set; } = 100;

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
        /// lengthens shutdown time.
        /// </summary>
        public bool TakeStateCheckpointWhenStoppingPartition { get; set; } = true;

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
        /// Whether to use the alternate object store implementation.
        /// </summary>
        public bool UseAlternateObjectStore { get; set; } = false;

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

            if (this.StorageChoice == StorageChoices.Faster && this.LoadInformationAzureTableName != null)
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
                Microsoft.Azure.Storage.NameValidator.ValidateContainerName(taskhubName.ToLowerInvariant());
                Microsoft.Azure.Storage.NameValidator.ValidateBlobName(taskhubName);
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