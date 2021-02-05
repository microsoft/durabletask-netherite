// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using DurableTask.Netherite;
    using Microsoft.Extensions.Logging;

    public static class TestConstants
    {
        public static string GetEventHubsConnectionString()
            // use the eventhubs connection string, if specified in environment, or else use the in-memory emulation with 4 partitions
            => GetTestSetting("EventHubsConnection", "MemoryF:4");

        public static string GetAzureStorageConnectionString()
            // use the storage connection string, if specified in the environment, or else use the storage emulator
            => GetTestSetting("AzureWebJobsStorage", "UseDevelopmentStorage=true;");

        static string GetTestSetting(string name, string defaultValue)
        {
            var envSetting = Environment.GetEnvironmentVariable(name);
            return envSetting ?? defaultValue;
        }

        public static string GetTestTaskHubName()
        {
            return "test-taskhub";
        }

        public static NetheriteOrchestrationServiceSettings GetNetheriteOrchestrationServiceSettings()
        {
            return new NetheriteOrchestrationServiceSettings
            {
                ResolvedTransportConnectionString = GetEventHubsConnectionString(),
                ResolvedStorageConnectionString = GetStorageConnectionString(),
                HubName = GetTestTaskHubName(),
                TransportLogLevelLimit = LogLevel.Trace,
                StorageLogLevelLimit = LogLevel.Trace,
                LogLevelLimit = LogLevel.Trace,
                EventLogLevelLimit = LogLevel.Trace,
                WorkItemLogLevelLimit = LogLevel.Trace,
                TakeStateCheckpointWhenStoppingPartition = true,  // set to false for testing recovery from log
                UseAlternateObjectStore = false,                  // set to true to bypass FasterKV; default is false
                MaxTimeMsBetweenCheckpoints = 1000000000,         // set this low for testing frequent checkpointing
                //MaxNumberBytesBetweenCheckpoints = 10000000, // set this low for testing frequent checkpointing
                //MaxNumberEventsBetweenCheckpoints = 10, // set this low for testing frequent checkpointing
            };
        }

        public static LogLevel UnitTestLogLevel = LogLevel.Trace;

        public static NetheriteOrchestrationService GetTestOrchestrationService(ILoggerFactory loggerFactory) 
            => new NetheriteOrchestrationService(GetNetheriteOrchestrationServiceSettings(), loggerFactory);

        internal static TestOrchestrationHost GetTestOrchestrationHost(ILoggerFactory loggerFactory)
            => new TestOrchestrationHost(GetNetheriteOrchestrationServiceSettings(), loggerFactory);


        public static bool DeleteStorageBeforeRunningTests => true; // set to false for testing log-based recovery

        public static string GetStorageConnectionString()
        {
            return GetAzureStorageConnectionString();
        }

        public static string GetPremiumStorageConnectionString()
        {
            // we are using the same storage in the unit tests
            return GetAzureStorageConnectionString();
        }
    }
}
