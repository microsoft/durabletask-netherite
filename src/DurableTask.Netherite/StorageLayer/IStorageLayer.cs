// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Scaling;

    /// <summary>
    /// The functionality for storing and recovering partition states.
    /// </summary>
    interface IStorageLayer
    {
        /// <summary>
        /// Tries to loads the task hub parameters.
        /// </summary>
        /// <returns>The parameters for the task hub, or null if the task hub does not exist.</returns>
        Task<TaskhubParameters> TryLoadTaskhubAsync(bool throwIfNotFound);

        /// <summary>
        /// Creates this taskhub in storage, if it does not already exist.
        /// </summary>
        /// <returns>true if the taskhub was actually created, false if it already existed.</returns>
        Task<bool> CreateTaskhubIfNotExistsAsync();

        /// <summary>
        /// Deletes this taskhub and all of its associated data in storage.
        /// </summary>
        /// <returns>after the taskhub has been deleted from storage.</returns>
        Task DeleteTaskhubAsync();

         /// <summary>
        /// The location where taskhub data is stored in storage.  
        /// </summary>
        /// <returns>The name of the Azure container, and the path prefix within the container.</returns>
        (string containerName, string path) GetTaskhubPathPrefix(TaskhubParameters parameters);

        /// <summary>
        /// Creates a <see cref="IPartitionState"/> object that represents the partition state.
        /// </summary>
        /// <returns></returns>
        IPartitionState CreatePartitionState(TaskhubParameters parameters);

        /// <summary>
        /// Where to publish the load information to. Is null if load should not be published.
        /// </summary>
        ILoadPublisherService LoadPublisher { get; }
    }
}
