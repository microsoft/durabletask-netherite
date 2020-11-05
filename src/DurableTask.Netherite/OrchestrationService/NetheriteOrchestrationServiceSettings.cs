// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        /// Gets or sets the connection string for the event hubs namespace, if needed.
        /// </summary>
        public string EventHubsConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the EventProcessor management
        /// </summary>
        public string EventProcessorManagement { get; set; } = "EventHubs";

        /// <summary>
        /// Gets or sets the connection string for the Azure storage account, supporting all types of blobs, and table storage.
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the connection string for a premium Azure storage account supporting page blobs only.
        /// </summary>
        public string PremiumStorageConnectionString { get; set; }

        [JsonIgnore]
        internal bool UsePremiumStorage => !string.IsNullOrEmpty(this.PremiumStorageConnectionString);

        /// <summary>
        /// The name of the taskhub. Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public string HubName { get; set; }

        /// <summary>
        /// Gets or sets the identifier for the current worker.
        /// </summary>
        public string WorkerId { get; set; } = Environment.MachineName;

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
        /// Gets or sets a flag indicating whether to enable caching of execution cursors to avoid replay.
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public bool ExtendedSessionsEnabled { get; set; } = true;

        /// <summary>
        /// Whether we should carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew.
        /// </summary>
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
        public bool UsePSFQueries { get; set; } = true;

        /// <summary>
        /// Whether to use the alternate object store implementation.
        /// </summary>
        public bool UseAlternateObjectStore { get; set; } = false;

        /// <summary>
        /// Forces steps to pe persisted before applying their effects, thus disabling all speculation.
        /// </summary>
        public bool PersistStepsFirst { get; set; } = false;

        /// <summary>
        /// Whether to use JSON serialization for eventhubs packets.
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public JsonPacketUse UseJsonPackets { get; set; } = JsonPacketUse.Never;

        /// <summary>
        /// Which packets to send in JSON format.
        /// </summary>
        public enum JsonPacketUse
        {
            /// <summary>
            /// Never send packets in JSON format
            /// </summary>
            Never,

            /// <summary>
            /// Send packets to clients in JSON format
            /// </summary>
            ForClients,

            /// <summary>
            /// Send all packets in JSON format
            /// </summary>
            ForAll,
        }

        /// <summary>
        /// A lower limit on the severity level of trace events emitted by the transport layer.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel TransportLogLevelLimit { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the severity level of trace events emitted by the storage layer.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel StorageLogLevelLimit { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the severity level of event processor trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel EventLogLevelLimit { get; set; } = LogLevel.Warning;

        /// <summary>
        /// A lower limit on the severity level of work item trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel WorkItemLogLevelLimit { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the severity level of all other trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel LogLevelLimit { get; set; } = LogLevel.Information;

        /// <summary>
        /// Validates the specified <see cref="NetheriteOrchestrationServiceSettings"/> object.
        /// </summary>
        /// <param name="settings">The <see cref="NetheriteOrchestrationServiceSettings"/> object to validate.</param>
        /// <returns>Returns <paramref name="settings"/> if successfully validated.</returns>
        public static NetheriteOrchestrationServiceSettings Validate(NetheriteOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (string.IsNullOrEmpty(settings.EventHubsConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.EventHubsConnectionString));
            }

            TransportConnectionString.Parse(settings.EventHubsConnectionString, out var storage, out var transport, out int? numPartitions);

            if (storage != TransportConnectionString.StorageChoices.Memory || transport != TransportConnectionString.TransportChoices.Memory)
            {
                if (string.IsNullOrEmpty(settings.StorageConnectionString))
                {
                    throw new ArgumentNullException(nameof(settings.StorageConnectionString));
                }
            }

            if ((transport != TransportConnectionString.TransportChoices.EventHubs))
            {
                if (numPartitions < 1 || numPartitions > 32)
                {
                    throw new ArgumentOutOfRangeException(nameof(settings.EventHubsConnectionString));
                }
            }
            else
            {
                if (string.IsNullOrEmpty(TransportConnectionString.EventHubsNamespaceName(settings.EventHubsConnectionString)))
                {
                    throw new FormatException(nameof(settings.EventHubsConnectionString));
                }
            }

            if (settings.MaxConcurrentOrchestratorFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentOrchestratorFunctions));
            }

            if (settings.MaxConcurrentActivityFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentActivityFunctions));
            }

            return settings;
        }      
    }
}
