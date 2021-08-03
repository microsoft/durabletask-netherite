﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Threading.Tasks;

    /// <summary>
    /// Top-level functionality for starting and stopping the transport back-end on a machine.
    /// </summary>
    public interface ITaskHub
    {
        /// <summary>
        /// Tests whether this taskhub exists in storage.
        /// </summary>
        /// <returns>true if this taskhub has been created in storage.</returns>
        Task<bool> ExistsAsync();

        /// <summary>
        /// Creates this taskhub in storage.
        /// </summary>
        /// <returns>true if the taskhub was actually created, false if it already existed.</returns>
        Task<bool> CreateIfNotExistsAsync();

        /// <summary>
        /// Deletes this taskhub and all of its associated data in storage.
        /// </summary>
        /// <returns>after the taskhub has been deleted from storage.</returns>
        Task DeleteAsync();

        /// <summary>
        /// Starts the transport backend. Creates a client and registers with the transport provider so partitions may be placed on this host.
        /// </summary>
        /// <returns>After the transport backend has started and created the client.</returns>
        Task StartAsync();

        /// <summary>
        /// Stops the transport backend.
        /// </summary>
        /// <returns>After the transport backend has stopped.</returns>
        Task StopAsync();
    }
}
