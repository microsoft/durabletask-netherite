// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using DurableTask.Netherite;
    using Microsoft.Extensions.Logging;

    public static class TestConstants
    {   
        public const string StorageConnectionName ="AzureWebJobsStorage";
        public const string EventHubsConnectionName ="EventHubsConnection";
        public const string DefaultTaskHubName ="test-taskhub";
        
        public static void ValidateEnvironment(bool requiresTransportSpec)
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(StorageConnectionName)))
            {
                throw new NetheriteConfigurationException($"To run tests, environment must define '{StorageConnectionName}'");
            }
            if (requiresTransportSpec && string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EventHubsConnectionName)))
            {
                throw new NetheriteConfigurationException($"To run tests, environment must define '{EventHubsConnectionName}'");
            }
        }

        public static NetheriteOrchestrationServiceSettings GetNetheriteOrchestrationServiceSettings(string emulationSpec = null)
        {
            var settings = new NetheriteOrchestrationServiceSettings
            {
                StorageConnectionName = StorageConnectionName,
                EventHubsConnectionName = emulationSpec ?? EventHubsConnectionName,
                HubName = DefaultTaskHubName,
                TransportLogLevelLimit = LogLevel.Trace,
                StorageLogLevelLimit = LogLevel.Trace,
                LogLevelLimit = LogLevel.Trace,
                EventLogLevelLimit = LogLevel.Trace,
                WorkItemLogLevelLimit = LogLevel.Trace,
                ClientLogLevelLimit = LogLevel.Trace,
                LoadMonitorLogLevelLimit = LogLevel.Trace,
                PartitionCount = 6,
                ThrowExceptionOnInvalidDedupeStatus = true,
                TakeStateCheckpointWhenStoppingPartition = true,  // set to false for testing recovery from log
                UseAlternateObjectStore = false,                  // set to true to bypass FasterKV; default is false
                IdleCheckpointFrequencyMs = 1000000000,         // set this low for testing frequent checkpointing
                //MaxNumberBytesBetweenCheckpoints = 10000000, // set this low for testing frequent checkpointing
                //MaxNumberEventsBetweenCheckpoints = 10, // set this low for testing frequent checkpointing
            };

            // uncomment the following for testing FASTER using local files only
            //settings.ResolvedTransportConnectionString = "SingleHost";
            //settings.ResolvedStorageConnectionString = "";
            //settings.UseLocalDirectoryForPartitionStorage = $"{Environment.GetEnvironmentVariable("temp")}\\FasterTestStorage";

            settings.Validate(new ConnectionNameToConnectionStringResolver((name) => Environment.GetEnvironmentVariable(name)));
            settings.TestHooks = new TestHooks();

            return settings;
        }

        public static LogLevel UnitTestLogLevel = LogLevel.Trace;

        public static NetheriteOrchestrationService GetTestOrchestrationService(ILoggerFactory loggerFactory) 
            => new NetheriteOrchestrationService(GetNetheriteOrchestrationServiceSettings(), loggerFactory);

        internal static TestOrchestrationHost GetTestOrchestrationHost(ILoggerFactory loggerFactory)
            => new TestOrchestrationHost(GetNetheriteOrchestrationServiceSettings(), loggerFactory);

        internal static bool UsesEmulation(this NetheriteOrchestrationServiceSettings settings)
        {
            return TransportConnectionString.IsPseudoConnectionString(settings.EventHubsConnectionName);
        }
    }
}
