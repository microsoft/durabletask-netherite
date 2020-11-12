// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Netherite;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class NetheriteProvider : DurabilityProvider
    {
        public NetheriteOrchestrationService Service { get; }
        public NetheriteOrchestrationServiceSettings Settings { get; }

        public NetheriteProvider(
            NetheriteOrchestrationService service,
            NetheriteOrchestrationServiceSettings settings)
            : base("Netherite", service, service, settings.StorageConnectionString)
        {
            this.Service = service;
            this.Settings = settings;
        }

        public override JObject ConfigurationJson => JObject.FromObject(this.Settings);

        public override bool SupportsEntities => true;

        public override bool SupportsPollFreeWait => true;

        public override bool GuaranteesOrderedDelivery => true;

        public override TimeSpan MaximumDelayTime { get; set; } = TimeSpan.MaxValue;

        /// <summary>
        /// The app setting containing the Azure Storage connection string.
        /// </summary>
        public override string ConnectionName => "StorageConnectionString";

        /// <inheritdoc/>
        public async override Task<string> RetrieveSerializedEntityState(EntityId entityId, JsonSerializerSettings serializerSettings)
        {
            var instanceId = ProviderUtils.GetSchedulerIdFromEntityId(entityId);
            OrchestrationState state = await this.Service.GetOrchestrationStateAsync(instanceId, true, true);
            if (ProviderUtils.TryGetEntityStateFromSerializedSchedulerState(state, serializerSettings, out string result))
            {
                return result;
            }
            else
            {
                return null;
            }
        }

        /// <inheritdoc/>
        public async override Task<IList<OrchestrationState>> GetOrchestrationStateWithInputsAsync(string instanceId, bool showInput = true)
        {
            var result = new List<OrchestrationState>();
            var state = await this.Service.GetOrchestrationStateAsync(instanceId, showInput, true);
            if (state != null)
            {
                result.Add(state);
            }

            return result;
        }

        /// <inheritdoc/>
        public async override Task<PurgeHistoryResult> PurgeInstanceHistoryByInstanceId(string instanceId)
        {
            var numberInstancesDeleted = await this.Service.PurgeInstanceHistoryAsync(instanceId);
            return new PurgeHistoryResult(numberInstancesDeleted);
        }

        /// <inheritdoc/>
        public override Task<int> PurgeHistoryByFilters(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            return this.Service.PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus);
        }

        /// <inheritdoc/>
        public async override Task<OrchestrationStatusQueryResult> GetOrchestrationStateWithPagination(OrchestrationStatusQueryCondition condition, CancellationToken cancellationToken)
        {
            var instanceQuery = new InstanceQuery(
                    runtimeStatus: condition.RuntimeStatus?.Select(p => (OrchestrationStatus)Enum.Parse(typeof(OrchestrationStatus), p.ToString())).ToArray(),
                    createdTimeFrom: (condition.CreatedTimeFrom == default) ? (DateTime?)null : condition.CreatedTimeFrom.ToUniversalTime(),
                    createdTimeTo: (condition.CreatedTimeTo == default) ? (DateTime?)null : condition.CreatedTimeTo.ToUniversalTime(),
                    instanceIdPrefix: condition.InstanceIdPrefix,
                    fetchInput: condition.ShowInput);

            InstanceQueryResult result = await this.Service.QueryOrchestrationStatesAsync(instanceQuery, condition.PageSize, condition.ContinuationToken, cancellationToken);

            return new OrchestrationStatusQueryResult()
            {
                DurableOrchestrationState = result.Instances.Select(ostate => ProviderUtils.ConvertOrchestrationStateToStatus(ostate)).ToList(),
                ContinuationToken = result.ContinuationToken,
            };
        }
    }
}
