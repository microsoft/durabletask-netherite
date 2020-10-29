//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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
