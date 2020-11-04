// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Azure.EventHubs;

    /// <summary>
    /// Encapsulates how the transport connection string setting is interpreted.
    /// </summary>
    public static class TransportConnectionString
    {
        /// <summary>
        /// Configuration options for the storage component
        /// </summary>
        public enum StorageChoices
        {
            /// <summary>
            /// Does not store any state to durable storage, just keeps it in memory. 
            /// Intended for testing scenarios.
            /// </summary>
            Memory = 0,

            /// <summary>
            /// Uses the Faster key-value store.
            /// </summary>
            Faster = 1,
        }

        /// <summary>
        /// Configuration options for the transport component
        /// </summary>
        public enum TransportChoices
        {
            /// <summary>
            /// Passes messages through memory and puts all partitions on a single host
            /// Intended for testing scenarios.
            /// </summary>
            Memory = 0,

            /// <summary>
            /// Passes messages through eventhubs; can distribute over multiple machines via
            /// the eventhubs EventProcessor.
            /// </summary>
            EventHubs = 1,
        }

        /// <summary>
        /// Determines the components to use given a transport connection string.
        /// </summary>
        public static void Parse(string transportConnectionString, out StorageChoices storage, out TransportChoices transport, out int? numPartitions)
        {
            if (transportConnectionString.StartsWith("Memory"))
            {
                transport = TransportChoices.Memory;
                storage = transportConnectionString.StartsWith("MemoryF") ? StorageChoices.Faster : StorageChoices.Memory;
                numPartitions = int.Parse(transportConnectionString.Substring(transportConnectionString.IndexOf(":") + 1));       
            }
            else
            {
                transport = TransportChoices.EventHubs;
                storage = StorageChoices.Faster;
                numPartitions = null; // number of partitions is detected dynamically, not specified by transportConnectionString
            }            
        }
 
        /// <summary>
        /// Returns the name of the eventhubs namespace
        /// </summary>
        public static string EventHubsNamespaceName(string transportConnectionString)
        {
            var builder = new EventHubsConnectionStringBuilder(transportConnectionString);
            var host = builder.Endpoint.Host;
            return host.Substring(0, host.IndexOf('.'));
        }
    }
}
