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
        public const string TaskHubName ="test-taskhub";
        
        public static void ValidateEnvironment()
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(StorageConnectionName)))
            {
                throw new InvalidOperationException($"To run tests, environment must define '{StorageConnectionName}'");
            }
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EventHubsConnectionName)))
            {
                throw new InvalidOperationException($"To run tests, environment must define '{EventHubsConnectionName}'");
            }
        }

        public static NetheriteOrchestrationServiceSettings GetNetheriteOrchestrationServiceSettings()
        {
            var settings = new NetheriteOrchestrationServiceSettings
            {
                StorageConnectionName = StorageConnectionName,
                EventHubsConnectionName = EventHubsConnectionName,
                HubName = TaskHubName,
                TransportLogLevelLimit = LogLevel.Trace,
                StorageLogLevelLimit = LogLevel.Trace,
                LogLevelLimit = LogLevel.Trace,
                EventLogLevelLimit = LogLevel.Trace,
                WorkItemLogLevelLimit = LogLevel.Trace,
                PartitionCount = 12,
                TakeStateCheckpointWhenStoppingPartition = true,  // set to false for testing recovery from log
                UseAlternateObjectStore = false,                  // set to true to bypass FasterKV; default is false
                MaxTimeMsBetweenCheckpoints = 1000000000,         // set this low for testing frequent checkpointing
                //MaxNumberBytesBetweenCheckpoints = 10000000, // set this low for testing frequent checkpointing
                //MaxNumberEventsBetweenCheckpoints = 10, // set this low for testing frequent checkpointing
            };

            // uncomment the following for testing FASTER using local files only
            //settings.ResolvedTransportConnectionString = "MemoryF";
            //settings.ResolvedStorageConnectionString = "";
            //settings.UseLocalDirectoryForPartitionStorage = $"{Environment.GetEnvironmentVariable("temp")}\\FasterTestStorage";

            settings.Validate((name) => Environment.GetEnvironmentVariable(name));

            return settings;
        }

        public static LogLevel UnitTestLogLevel = LogLevel.Trace;

        public static NetheriteOrchestrationService GetTestOrchestrationService(ILoggerFactory loggerFactory) 
            => new NetheriteOrchestrationService(GetNetheriteOrchestrationServiceSettings(), loggerFactory);

        internal static TestOrchestrationHost GetTestOrchestrationHost(ILoggerFactory loggerFactory)
            => new TestOrchestrationHost(GetNetheriteOrchestrationServiceSettings(), loggerFactory);


        public static bool DeleteStorageBeforeRunningTests => true; // set to false for testing log-based recovery
    }
}
