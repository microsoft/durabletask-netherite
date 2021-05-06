﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using DurableTask.Core;
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
        /// The name to use for the Azure table with the load information
        /// </summary>
        public string LoadInformationAzureTableName { get; set; } = "DurableTaskPartitions";

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
        /// Gets or sets the partition management option
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public PartitionManagementOptions PartitionManagement { get; set; } = PartitionManagementOptions.EventProcessorHost;

        /// <summary>
        /// Gets or sets a flag indicating whether to enable caching of execution cursors to avoid replay.
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public bool ExtendedSessionsEnabled { get; set; } = true;

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
        public long MaxTimeMsBetweenCheckpoints { get; set; } = 60 * 1000;

        /// <summary>
        /// Whether to use the Faster PSF support for handling queries.
        /// </summary>
        public bool UsePSFQueries { get; set; } = false;

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
        /// Forces steps to pe persisted before applying their effects, thus disabling all speculation.
        /// </summary>
        public bool PersistStepsFirst { get; set; } = false;

        /// <summary>
        /// Pack TaskMessages generated by a single work item for the same destination into a single event.
        /// </summary>
        public int PackPartitionTaskMessages { get; set; } = 100;

        /// <summary>
        /// Gets or sets the name used for resolving the premium Azure storage connection string, if used.
        /// </summary>
        public string PremiumStorageConnectionName { get; set; } = null;

        [JsonIgnore]
        internal bool UsePremiumStorage => !string.IsNullOrEmpty(this.PremiumStorageConnectionName);

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
        /// <param name="connectionStringResolver">The connection string resolver.</param>
        public void Validate(Func<string,string> connectionStringResolver)
        {
            if (string.IsNullOrEmpty(this.ResolvedStorageConnectionString))
            {
                if (string.IsNullOrEmpty(this.StorageConnectionName))
                {
                    throw new InvalidOperationException($"Must specify {nameof(this.StorageConnectionName)} for Netherite storage provider.");
                }

                this.ResolvedStorageConnectionString = connectionStringResolver(this.StorageConnectionName);

                if (string.IsNullOrEmpty(this.ResolvedStorageConnectionString))
                {
                    throw new InvalidOperationException($"Could not resolve {nameof(this.StorageConnectionName)}:{this.StorageConnectionName} for Netherite storage provider.");
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
                    this.ResolvedTransportConnectionString = connectionStringResolver(this.EventHubsConnectionName);

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
