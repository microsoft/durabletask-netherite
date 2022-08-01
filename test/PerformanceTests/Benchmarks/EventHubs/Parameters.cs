// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Identity.Client;

    public static class Parameters
    {
        // the name of the environment variable containing the event hubs connection string for the event hub
        // that we are pushing events to and pulling events from
        public const string EventHubsEnvVar = "EHNamespace";

        // EventHubs dimensions
        public const int MaxEventHubs = 10;
        public const int MaxPartitionsPerEventHub = 32;
        public const int MaxPartitions = MaxEventHubs * MaxPartitionsPerEventHub;
        public const int MaxPullers = MaxEventHubs * MaxPartitionsPerEventHub; // max 1 puller per partition

        // Event content
        public const int PayloadStringLength = 5;

        // Limit on the number of pending signals and orchestrations
        // Serves as flow control to avoid pulling more from event hub than what can be processed
        public const int PendingLimit = 3000;

        // Forwarding of events to entities
        public static bool SendEntitySignalForEachEvent = true;
        public const int NumberDestinationEntities = 100;

        // Starting of orchestrations for events
        public static bool StartOrchestrationForEachEvent = false;
        public static bool PlaceOrchestrationInstance = true;

        public static string EventHubConnectionString => Environment.GetEnvironmentVariable(EventHubsEnvVar);

        public static string EventHubName(int index) => $"hub{index}";

        // assignment of puller entities to eventhubs partitions
        public static string EventHubNameForPuller(int number) => EventHubName(number / MaxPartitionsPerEventHub);
        public static string EventHubPartitionIdForPuller(int number) => (number % MaxPartitionsPerEventHub).ToString();

        // assignment of producer entities to eventhubs
        public static string EventHubNameForProducer(int number) => EventHubName((number / MaxPartitionsPerEventHub) % MaxEventHubs);
    }
}