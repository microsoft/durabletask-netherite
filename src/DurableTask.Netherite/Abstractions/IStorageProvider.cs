// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// The functionality for storing and recovering partition states.
    /// </summary>
    interface IStorageProvider
    {
        /// <summary>
        /// Creates a <see cref="IPartitionState"/> object that represents the partition state.
        /// </summary>
        /// <returns></returns>
        IPartitionState CreatePartitionState();

        /// <summary>
        /// Deletes all partition states.
        /// </summary>
        /// <returns></returns>
        Task DeleteTaskhubAsync(string pathPrefix);
    }
}
