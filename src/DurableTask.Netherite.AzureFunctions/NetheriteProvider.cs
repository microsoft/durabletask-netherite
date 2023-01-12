// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Netherite;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class NetheriteProvider : DurabilityProvider
    {
        public NetheriteOrchestrationService Service { get; }
        public NetheriteOrchestrationServiceSettings Settings { get; }

        public NetheriteProvider(
            NetheriteOrchestrationService service,
            NetheriteOrchestrationServiceSettings settings)
            : base("Netherite", service, service, settings.StorageConnectionName)
        {
            this.Service = service;
            this.Settings = settings;
        }

        public override JObject ConfigurationJson => JObject.FromObject(this.Settings);

        public override bool SupportsEntities => true;

        public override bool SupportsPollFreeWait => true;

        public override bool GuaranteesOrderedDelivery => true;

        public override bool SupportsImplicitEntityDeletion => true;

        public override TimeSpan MaximumDelayTime { get; set; } = TimeSpan.MaxValue;

        public override string EventSourceName => "DurableTask-Netherite";

        /// <inheritdoc/>
        public async override Task<string> RetrieveSerializedEntityState(EntityId entityId, JsonSerializerSettings serializerSettings)
        {
            var instanceId = ProviderUtils.GetSchedulerIdFromEntityId(entityId);
            OrchestrationState state = await ((IOrchestrationServiceQueryClient)this.Service).GetOrchestrationStateAsync(instanceId, true, true);
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
            var state = await ((IOrchestrationServiceQueryClient)this.Service).GetOrchestrationStateAsync(instanceId, showInput, true);
            if (state != null)
            {
                result.Add(state);
            }

            return result;
        }

        /// <inheritdoc/>
        public async override Task<PurgeHistoryResult> PurgeInstanceHistoryByInstanceId(string instanceId)
        {
            var numberInstancesDeleted = await ((IOrchestrationServiceQueryClient)this.Service).PurgeInstanceHistoryAsync(instanceId);
            return new PurgeHistoryResult(numberInstancesDeleted);
        }

        /// <inheritdoc/>
        public override Task<int> PurgeHistoryByFilters(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            return ((IOrchestrationServiceQueryClient)this.Service).PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus);
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

            InstanceQueryResult result = await ((IOrchestrationServiceQueryClient)this.Service).QueryOrchestrationStatesAsync(instanceQuery, condition.PageSize, condition.ContinuationToken, cancellationToken);

            return new OrchestrationStatusQueryResult()
            {
                DurableOrchestrationState = result.Instances.Select(ostate => ProviderUtils.ConvertOrchestrationStateToStatus(ostate)).ToList(),
                ContinuationToken = result.ContinuationToken,
            };
        }

        public override bool TryGetScaleMonitor(
            string functionId,
            string functionName,
            string hubName,
            string storageConnectionString,
            out IScaleMonitor scaleMonitor)
        {
            if (this.Service.TryGetScalingMonitor(out var monitor))
            {
                scaleMonitor = new NetheriteScaleMonitor(monitor);
                monitor.InformationTracer($"ScaleMonitor Constructed, Descriptor.Id={scaleMonitor.Descriptor.Id}");
                return true;
            }
            else
            {
                scaleMonitor = null;
                return false;
            }
        }
    }
}
#endif