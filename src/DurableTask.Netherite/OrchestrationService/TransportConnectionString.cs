// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime;
    using System.Text;

    /// <summary>
    /// Encapsulates how the transport connection string setting is interpreted.
    /// </summary>
    public static class TransportConnectionString
    {
        /// <summary>
        /// Determines the components to use given a transport connection string.
        /// </summary>
        public static bool IsPseudoConnectionString(string connectionString)
        {
            switch ((connectionString ?? "").ToLowerInvariant().Trim())
            {
                case "memory":
                case "singlehost":
                case "memoryf": // for backwards compatibility
                    return true;

                default:
                    return false;
            }
        }

        /// <summary>
        /// Determines the components to use given a transport connection string.
        /// </summary>
        public static void Parse(string transportConnectionString, out StorageChoices storage, out TransportChoices transport)
        {
            switch (transportConnectionString.ToLowerInvariant().Trim())
            {
                case "memory":
                    transport = TransportChoices.SingleHost;
                    storage = StorageChoices.Memory;
                    return;

                case "singlehost":
                case "memoryf": // for backwards compatibility
                    transport = TransportChoices.SingleHost;
                    storage = StorageChoices.Faster;
                    return;

                default:
                    transport = TransportChoices.EventHubs;
                    storage = StorageChoices.Faster;
                    return;
            }
        }
    }
}
