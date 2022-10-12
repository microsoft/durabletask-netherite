// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Configuration options for the storage layer
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

        /// <summary>
        /// Uses a custom dependency-injected storage layer
        /// </summary>
        Custom = 2,
    }
}