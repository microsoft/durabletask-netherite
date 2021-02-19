// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Netherite;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class NetheriteProvider : DurabilityProvider, ProviderUtils.IProviderWithAutoScaling
    {
        public NetheriteOrchestrationService Service { get; }
        public NetheriteOrchestrationServiceSettings Settings { get; }

        public NetheriteProvider(
            NetheriteOrchestrationService service,
            NetheriteOrchestrationServiceSettings settings)
            : base("Netherite", service, service, settings.ResolvedStorageConnectionString)
        {
            this.Service = service;
            this.Settings = settings;
        }

        public override JObject ConfigurationJson => JObject.FromObject(this.Settings);

        public override bool SupportsEntities => true;

        public override bool SupportsPollFreeWait => true;

        public override bool GuaranteesOrderedDelivery => true;

        public override TimeSpan MaximumDelayTime { get; set; } = TimeSpan.MaxValue;

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

        bool ProviderUtils.IProviderWithAutoScaling.TryGetScaleMonitor(DurableTaskExtension extension, out IScaleMonitor scaleMonitor)
        {
            if (this.Service.TryGetScalingMonitor(out var monitor))
            {
                scaleMonitor = new ScaleMonitor(monitor, extension);
                return true;
            }
            else
            {
                scaleMonitor = null;
                return false;
            }
        }

        class ScaleMonitor : IScaleMonitor
        {
            readonly ScalingMonitor scalingMonitor;
            readonly DurableTaskExtension extension;

            public ScaleMonitor(ScalingMonitor scalingMonitor, DurableTaskExtension extension)
            {
                this.scalingMonitor = scalingMonitor;
                this.extension = extension;
                this.Descriptor = new ScaleMonitorDescriptor($"DurableTaskTrigger-Netherite-{this.scalingMonitor.TaskHubName}".ToLower());
            }

            public ScaleMonitorDescriptor Descriptor { get; private set; }

            class NetheriteScaleMetrics : ScaleMetrics
            {
                public ScalingMonitor.Metrics Metrics { get; set; }
            }

            async Task<ScaleMetrics> IScaleMonitor.GetMetricsAsync()
            {
                try
                {
                    return new NetheriteScaleMetrics()
                    {
                        Metrics = await this.scalingMonitor.CollectMetrics()
                    };
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    this.extension.TraceWarningEvent(this.scalingMonitor.TaskHubName,
                                            string.Empty,
                                            string.Empty,
                                            $"Netherite backend: IScaleMonitor.GetMetricsAsync() failed: {e}");
                    throw;
                }
            }

            ScaleStatus IScaleMonitor.GetScaleStatus(ScaleStatusContext context)
            {
                try
                {
                    var metrics = ((NetheriteScaleMetrics)context.Metrics.Last()).Metrics;
                    ScaleRecommendation recommendation = this.scalingMonitor.GetScaleRecommendation(context.WorkerCount, metrics);
                    ScaleStatus scaleStatus = new ScaleStatus();
                    bool writeToUserLogs;

                    switch (recommendation.Action)
                    {
                        case ScaleAction.AddWorker:
                            scaleStatus.Vote = ScaleVote.ScaleOut;
                            writeToUserLogs = true;
                            break;
                        case ScaleAction.RemoveWorker:
                            scaleStatus.Vote = ScaleVote.ScaleIn;
                            writeToUserLogs = true;
                            break;
                        default:
                            scaleStatus.Vote = ScaleVote.None;
                            writeToUserLogs = false;
                            break;
                    }

                    if (scaleStatus.Vote != ScaleVote.None)
                        this.extension.TraceInformationalEvent(
                                            this.scalingMonitor.TaskHubName,
                                            string.Empty,
                                            string.Empty,
                                            $"Durable Functions Trigger Scale Decision: {scaleStatus.Vote.ToString()}, Reason: {recommendation.Reason}",
                                            writeToUserLogs: writeToUserLogs);

                    return scaleStatus;
                }
                catch(Exception e) when (!Utils.IsFatal(e))
                {
                    this.extension.TraceWarningEvent(this.scalingMonitor.TaskHubName,
                                            string.Empty,
                                            string.Empty,
                                            $"Netherite backend: IScaleMonitor.GetScaleStatus failed: {e}");
                    throw;
                }
            }
        }
    }
}
