// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Tests
{
    using System;
    using DurableTask.Netherite;
    using Microsoft.Extensions.Logging;

    static class TestHelpers
    {
        public static NetheriteOrchestrationServiceSettings GetNetheriteOrchestrationServiceSettings()
        {
            return new NetheriteOrchestrationServiceSettings
            {
                EventHubsConnectionString = GetEventHubsConnectionString(),
                StorageConnectionString = GetStorageConnectionString(),
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

        public static TestOrchestrationHost GetTestOrchestrationHost(ILoggerFactory loggerFactory)
            => new TestOrchestrationHost(GetNetheriteOrchestrationServiceSettings(), loggerFactory);

        public static string GetTestTaskHubName()
        {
            return "test-taskhub";
            //Configuration appConfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            //return appConfig.AppSettings.Settings["TaskHubName"].Value;
        }

        public const string DurableTaskTestPrefix = "DurableTaskTest";

        public static bool DeleteStorageBeforeRunningTests => true; // set to false for testing log-based recovery

        public static string GetAzureStorageConnectionString() => GetTestSetting("StorageConnectionString", true);

        public static string GetStorageConnectionString()
        {
            // NOTE: If using the local file system, modify GetEventHubsConnectionString use one of the memory options.
            // return FasterStorage.LocalFileStorageConnectionString;

            return GetAzureStorageConnectionString();
        }

        public static string GetPremiumStorageConnectionString()
        {
            // we are using the same storage in the unit tests
            return GetAzureStorageConnectionString();
        }

        public static string GetEventHubsConnectionString()
        {
            // NOTE: If using any of the memory options, modify GetStorageConnectionString to use the local file system.

            // Memory means TransportChoices.Memory and StorageChoices.Memory
            // return "Memory:1";
            // return "Memory:4";
            // return "Memory:32";

            // MemoryF means TransportChoices.Memory and StorageChoices.Faster
            // return "MemoryF:1";
            // return "MemoryF:4";
            // return "MemoryF:32";

            // using an actual connection string means TransportChoices.EventHubs and StorageChoices.Faster
            return GetTestSetting("EventHubsConnectionString", false);
        }

        static string GetTestSetting(string name, bool require)
        {
            var setting = Environment.GetEnvironmentVariable(DurableTaskTestPrefix + name);
            return !string.IsNullOrEmpty(setting) || !require
                ? setting
                : throw new ArgumentNullException($"The environment variable {DurableTaskTestPrefix + name} must be defined for the tests to run");
        }
    }
}
