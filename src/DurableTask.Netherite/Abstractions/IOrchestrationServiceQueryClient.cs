// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Query interface for the orchestration service. These should really be part of IOrchestrationServiceClient, but are not for historical reasons.
    /// </summary>
    public interface IOrchestrationServiceQueryClient
    {
        /// <summary>
        /// Gets the current state of an instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        /// <param name="fetchOutput">If set, fetch and return the output for the orchestration instance.</param>
        /// <returns>The state of the instance, or null if not found.</returns>
        Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, bool fetchInput = true, bool fetchOutput = true);

        /// <summary>
        /// Gets the state of all orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        Task<IList<OrchestrationState>> GetAllOrchestrationStatesAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Gets the state of selected orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? CreatedTimeFrom = default,
                                                                          DateTime? CreatedTimeTo = default,
                                                                          IEnumerable<OrchestrationStatus> RuntimeStatus = default,
                                                                          string InstanceIdPrefix = default,
                                                                          CancellationToken CancellationToken = default);

        /// <summary>
        /// Purge history for an orchestration with a specified instance id.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        Task<int> PurgeInstanceHistoryAsync(string instanceId);

        /// <summary>
        /// Purge history for orchestrations that match the specified parameters.
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Purges history grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Purges history less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        Task<int> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus);

        /// <summary>
        /// Query orchestration instance states.
        /// </summary>
        /// <param name="instanceQuery">The query to perform.</param>
        /// <param name="pageSize">The page size.</param>
        /// <param name="continuationToken">The continuation token.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The result of the query.</returns>
        Task<InstanceQueryResult> QueryOrchestrationStatesAsync(InstanceQuery instanceQuery, int pageSize, string continuationToken, CancellationToken cancellationToken);
    }
}