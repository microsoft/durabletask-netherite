// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
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
        /// Alternatively, may contain special strings specifying the use of the emulator.
        /// </summary>
        public string EventHubsConnectionName { get; set; } = "EventHubsConnection";

        /// <summary>
        /// The resolved storage connection string. Is never serialized or deserialized.
        /// </summary>
        [JsonIgnore]
        public string ResolvedStorageConnectionString { get; set; }

        /// <summary>
        /// The resolved event hubs connection string. Is never serialized or deserialized.
        /// </summary>
        [JsonIgnore]
        public string ResolvedTransportConnectionString { get; set; }

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
        /// Limit on the amount of memory used by the FASTER cache
        /// </summary>
        public int? FasterCacheSizeMB { get; set; } = null;

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
        ///  Whether to keep the orchestration service running even if stop is called.
        ///  This is useful in a testing scenario, due to the inordinate time spent when shutting down EventProcessorHost.
        /// </summary>
        public bool KeepServiceRunning { get; set; } = false;

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
        /// Pack TaskMessages generated by a single work item for the same destination into a single event.
        /// </summary>
        public int PackPartitionTaskMessages { get; set; } = 100;

        /// <summary>
        /// A name for resolving a storage connection string to be used specifically for the page blobs, or null if page blobs are to be stored in the default account.
        /// </summary>
        public string PageBlobStorageConnectionName { get; set; } = null;

        /// <summary>
        /// The resolved page blob storage connection string, or null if page blobs are to be stored in the default account. Is never serialized or deserialized.
        /// </summary>
        [JsonIgnore]
        public string ResolvedPageBlobStorageConnectionString { get; set; }

        [JsonIgnore]
        internal bool UseSeparatePageBlobStorage => !string.IsNullOrEmpty(this.ResolvedPageBlobStorageConnectionString);

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
        /// A lower limit on the severity level of all other trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel LogLevelLimit { get; set; } = LogLevel.Debug;

        /// <summary>
        /// Validates the settings, throwing exceptions if there are issues.
        /// </summary>
        /// <param name="nameResolver">The resolver for environment variables.</param>
        public void Validate(Func<string,string> nameResolver)
        {
            if (string.IsNullOrEmpty(this.ResolvedStorageConnectionString))
            {
                if (string.IsNullOrEmpty(this.StorageConnectionName))
                {
                    throw new InvalidOperationException($"Must specify {nameof(this.StorageConnectionName)} for Netherite storage provider.");
                }

                this.ResolvedStorageConnectionString = nameResolver(this.StorageConnectionName);

                if (string.IsNullOrEmpty(this.ResolvedStorageConnectionString))
                {
                    throw new InvalidOperationException($"Could not resolve {nameof(this.StorageConnectionName)}:{this.StorageConnectionName} for Netherite storage provider.");
                }
            }

            if (string.IsNullOrEmpty(this.ResolvedPageBlobStorageConnectionString)
                && !string.IsNullOrEmpty(this.PageBlobStorageConnectionName))
            {
                this.ResolvedPageBlobStorageConnectionString = nameResolver(this.PageBlobStorageConnectionName);

                if (string.IsNullOrEmpty(this.ResolvedPageBlobStorageConnectionString))
                {
                    throw new InvalidOperationException($"Could not resolve {nameof(this.PageBlobStorageConnectionName)}:{this.PageBlobStorageConnectionName} for Netherite storage provider.");
                }
            }

            if (string.IsNullOrEmpty(this.ResolvedTransportConnectionString))
            {
                if (string.IsNullOrEmpty(this.EventHubsConnectionName))
                {
                    throw new InvalidOperationException($"Must specify {nameof(this.EventHubsConnectionName)} for Netherite storage provider.");
                }

                if (TransportConnectionString.IsEmulatorSpecification(this.EventHubsConnectionName))
                {
                    this.ResolvedTransportConnectionString = this.EventHubsConnectionName;
                }
                else
                {
                    this.ResolvedTransportConnectionString = nameResolver(this.EventHubsConnectionName);

                    if (string.IsNullOrEmpty(this.ResolvedTransportConnectionString))
                    {
                        throw new InvalidOperationException($"Could not resolve {nameof(this.EventHubsConnectionName)}:{this.EventHubsConnectionName} for Netherite storage provider.");
                    }
                }
            }

            TransportConnectionString.Parse(this.ResolvedTransportConnectionString, out var storage, out var transport);

            if (this.PartitionCount < 1 || this.PartitionCount > 32)
            {
                throw new ArgumentOutOfRangeException(nameof(this.PartitionCount));
            }

            if (transport == TransportConnectionString.TransportChoices.EventHubs)
            {
                // validates the connection string
                TransportConnectionString.EventHubsNamespaceName(this.ResolvedTransportConnectionString);            
            }

            if (this.MaxConcurrentOrchestratorFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(this.MaxConcurrentOrchestratorFunctions));
            }

            if (this.MaxConcurrentActivityFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(this.MaxConcurrentActivityFunctions));
            }
        }      
    }
}
