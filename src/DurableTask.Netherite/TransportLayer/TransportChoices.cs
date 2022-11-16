// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Configuration options for the transport layer
    /// </summary>
    public enum TransportChoices
    {
        /// <summary>
        /// Passes messages through memory and puts all partitions on a single host
        /// Intended primarily for testing scenarios, but can also provide a cheap and efficient option for
        /// workloads whose CPU and memory load do not require more than one worker.
        /// </summary>
        SingleHost = 0,

        /// <summary>
        /// Passes messages through eventhubs; can distribute over multiple machines via
        /// the eventhubs EventProcessor.
        /// </summary>
        EventHubs = 1,

        /// <summary>
        /// Uses a custom dependency-injected transport layer
        /// </summary>
        Custom = 2,
    }
}