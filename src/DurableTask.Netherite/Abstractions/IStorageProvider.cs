// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
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
        Task DeleteAllPartitionStatesAsync();
    }
}
