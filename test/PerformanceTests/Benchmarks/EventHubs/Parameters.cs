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
        public const string EventHubsEnvVar = "EHNamespace";

        public const int Destinations = 100;

        public const int MaxEventHubs = 10;
        public const int MaxPartitionsPerEventHub = 32;

        public const int MaxPartitions = MaxEventHubs * MaxPartitionsPerEventHub;

        public const int MaxProducers = MaxPartitions;

        public const int MaxPushersPerEventHub = 32;
        public const int MaxPullers = MaxEventHubs * MaxPushersPerEventHub;

        public static string EventHubConnectionString => Environment.GetEnvironmentVariable(EventHubsEnvVar);

        public static string EventHubName(int index) => $"hub{index}";

        public static string EventHubNameForPuller(int number) => EventHubName(number / MaxPartitionsPerEventHub);
        public static string EventHubPartitionIdForPuller(int number) => (number % MaxPartitionsPerEventHub).ToString();

        public static string EventHubNameForProducer(int number) => EventHubName(number / MaxPushersPerEventHub);


    }
}