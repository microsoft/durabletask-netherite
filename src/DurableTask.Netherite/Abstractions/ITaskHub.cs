// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        /// <returns>after the taskhub has been created in storage.</returns>
        Task CreateAsync();

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
        /// <param name="isForced">Whether to shut down as quickly as possible, or gracefully.</param>
        /// <returns>After the transport backend has stopped.</returns>
        Task StopAsync(bool isForced);
    }
}
