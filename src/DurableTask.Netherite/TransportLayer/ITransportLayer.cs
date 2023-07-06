// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;

    /// <summary>
    /// Top-level functionality for starting and stopping the transport layer on a machine.
    /// </summary>
    public interface ITransportLayer
    {
        /// <summary>
        /// Starts the transport backend. Throws an exception if taskhub does not exist in storage.
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns>the task hub parameters</returns>
        Task<TaskhubParameters> StartAsync();

        /// <summary>
        /// Creates a client. Must be called after StartAsync() and before StartWorkersAsync();
        /// </summary>
        /// <returns>After the transport backend has started and created the client.</returns>
        Task StartClientAsync();

        /// <summary>
        /// Starts the workers that process work items. Must be called after StartClientAsync();
        /// </summary>
        /// <returns>After the transport backend has started and created the client.</returns>
        Task StartWorkersAsync();

        /// <summary>
        /// Stops the transport backend.
        /// </summary>
        /// <param name="fatalExceptionObserved">Whether this stop was initiated because we have observed a fatal exception.</param>
        /// <returns>After the transport backend has stopped.</returns>
        Task StopAsync(bool fatalExceptionObserved);
    }
}
