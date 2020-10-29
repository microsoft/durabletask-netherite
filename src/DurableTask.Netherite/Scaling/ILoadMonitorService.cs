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

namespace DurableTask.Netherite.Scaling
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An interface for the load monitor service.
    /// </summary>
    public interface ILoadMonitorService
    {
        /// <summary>
        /// Publish the load of a partition to the service.
        /// </summary>
        /// <param name="loadInfo">A collection of load information for partitions</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A task indicating completion</returns>
        Task PublishAsync(Dictionary<uint, PartitionLoadInfo> loadInfo, CancellationToken cancellationToken);

        /// <summary>
        /// Delete all load information for a taskhub.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A task indicating completion</returns>
        Task DeleteIfExistsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Prepare the service for a taskhub.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A task indicating completion</returns>
        Task CreateIfNotExistsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Query all load information for a taskhub.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A task returning a dictionary with load information for the partitions</returns>
        Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken);
    }
}
