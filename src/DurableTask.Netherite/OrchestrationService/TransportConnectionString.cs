// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime;
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
        public static bool IsEmulatorSpecification(string specification)
        {
            return specification == "Memory" || specification == "MemoryF";         
        }

        /// <summary>
        /// Determines the components to use given a transport connection string.
        /// </summary>
        public static void Parse(string specification, out StorageChoices storage, out TransportChoices transport)
        {
            if (IsEmulatorSpecification(specification))
            {
                transport = TransportChoices.Memory;
                storage = specification == "MemoryF" ? StorageChoices.Faster : StorageChoices.Memory;
            }
            else
            {
                transport = TransportChoices.EventHubs;
                storage = StorageChoices.Faster;
            }            
        }
 
        /// <summary>
        /// Returns the name of the eventhubs namespace
        /// </summary>
        public static string EventHubsNamespaceName(string transportConnectionString)
        {
            try
            {
                var builder = new EventHubsConnectionStringBuilder(transportConnectionString);
                var host = builder.Endpoint.Host;
                return host.Substring(0, host.IndexOf('.'));
            }
            catch(Exception e)
            {
                throw new FormatException("Could not parse the specified Eventhubs namespace connection string.", e);
            }
        }
    }
}
