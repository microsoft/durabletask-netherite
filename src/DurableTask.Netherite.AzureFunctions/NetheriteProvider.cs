// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

        public override string EventSourceName => "DurableTask-Netherite";

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

        public override bool TryGetScaleMonitor(
            string functionId, 
            string functionName, 
            string hubName, 
            string storageConnectionString, 
            out IScaleMonitor scaleMonitor)
        {
            if (this.Service.TryGetScalingMonitor(out var monitor))
            {
                scaleMonitor = new ScaleMonitor(monitor);
                return true;
            }
            else
            {
                scaleMonitor = null;
                return false;
            }
        }

        public class NetheriteScaleMetrics : ScaleMetrics
        {
            public byte[] Metrics { get; set; }
        }

        class ScaleMonitor : IScaleMonitor<NetheriteScaleMetrics>
        {
            readonly ScalingMonitor scalingMonitor;
            readonly ScaleMonitorDescriptor descriptor;
            readonly DataContractSerializer serializer  = new DataContractSerializer(typeof(ScalingMonitor.Metrics));
            static Tuple<DateTime, NetheriteScaleMetrics> cachedMetrics;

            public ScaleMonitor(ScalingMonitor scalingMonitor)
            {
                this.scalingMonitor = scalingMonitor;
                this.descriptor = new ScaleMonitorDescriptor($"DurableTaskTrigger-Netherite-{this.scalingMonitor.TaskHubName}".ToLower());
            }

            public ScaleMonitorDescriptor Descriptor => this.descriptor;


            async Task<ScaleMetrics> IScaleMonitor.GetMetricsAsync()
            {
                return await this.GetMetricsAsync();
            }

            public async Task<NetheriteScaleMetrics> GetMetricsAsync()
            {
                // if we recently collected the metrics, return the cached result now.
                var cached = cachedMetrics;
                if (cached != null && DateTime.UtcNow - cached.Item1 < TimeSpan.FromSeconds(1.5))
                {
                    return cached.Item2;
                }
                
                var metrics = new NetheriteScaleMetrics();

                try
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    var collectedMetrics = await this.scalingMonitor.CollectMetrics();
                    sw.Stop();

                    var stream = new MemoryStream();
                    this.serializer.WriteObject(stream, collectedMetrics);
                    metrics.Metrics = stream.ToArray();

                    this.scalingMonitor.Logger.LogInformation(
                        "Collected scale info for {partitionCount} partitions at {time:o} in {latencyMs:F2}ms.",
                        collectedMetrics.LoadInformation.Count, collectedMetrics.Timestamp, sw.Elapsed.TotalMilliseconds);
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    this.scalingMonitor.Logger.LogError("IScaleMonitor.GetMetricsAsync() failed: {exception}", e);
                }

                cachedMetrics = new Tuple<DateTime, NetheriteScaleMetrics>(DateTime.UtcNow, metrics);
                return metrics;
            }

            ScaleStatus IScaleMonitor.GetScaleStatus(ScaleStatusContext context)
            {
                return this.GetScaleStatusCore(context.WorkerCount, context.Metrics?.Cast<NetheriteScaleMetrics>().ToArray());
            }

            public ScaleStatus GetScaleStatus(ScaleStatusContext<NetheriteScaleMetrics> context)
            {
                return this.GetScaleStatusCore(context.WorkerCount, context.Metrics?.ToArray());
            }

            ScaleStatus GetScaleStatusCore(int workerCount, NetheriteScaleMetrics[] metrics)
            {
                ScaleRecommendation recommendation;               
                try
                { 
                    if (metrics.Length == 0)
                    {
                        recommendation = new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: "missing metrics");
                    }
                    else
                    {
                        var stream = new MemoryStream(metrics[^1].Metrics);
                        var collectedMetrics = (ScalingMonitor.Metrics) this.serializer.ReadObject(stream);                 
                        recommendation = this.scalingMonitor.GetScaleRecommendation(workerCount, collectedMetrics);
                    }
                 }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    this.scalingMonitor.Logger.LogError("IScaleMonitor.GetScaleStatus() failed: {exception}", e);
                    recommendation = new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: "unexpected error");
                }

                ScaleStatus scaleStatus = new ScaleStatus();
                switch (recommendation?.Action)
                {
                    case ScaleAction.AddWorker:
                        scaleStatus.Vote = ScaleVote.ScaleOut;
                        break;
                    case ScaleAction.RemoveWorker:
                        scaleStatus.Vote = ScaleVote.ScaleIn;
                        break;
                    default:
                        scaleStatus.Vote = ScaleVote.None;
                        break;
                }

                this.scalingMonitor.Logger.LogInformation(
                    "Netherite autoscaler recommends: {scaleRecommendation} from: {workerCount} because: {reason}",
                    scaleStatus.Vote.ToString(), workerCount, recommendation.Reason);

                return scaleStatus;
            }
        }
    }
}
