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

        NetheriteMetricsProvider metricsProvider;
        ILoadPublisherService loadPublisher;

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
                scaleMonitor = new ScaleMonitor(monitor, $"{functionId}-{functionName}-{hubName}");
                monitor.InformationTracer($"ScaleMonitor Constructed, Descriptor.Id={scaleMonitor.Descriptor.Id}");
                return true;
            }
            else
            {
                scaleMonitor = null;
                return false;
            }
        }

        public override bool TryGetTargetScaler(
            string functionId,
            string functionName,
            string hubName,
            string connectionName,
            out ITargetScaler targetScaler)
        {           
            // Target Scaler is created per function id. And they share the same NetheriteMetricsProvider.
            if ( this.metricsProvider == null)
            {
                this.loadPublisher ??= this.Service.GetLoadPublisher();
                this.metricsProvider = this.Service.GetNetheriteMetricsProvider(this.loadPublisher, this.Settings.EventHubsConnection);
            }
            
            targetScaler = new NetheriteTargetScaler(functionId, this.metricsProvider, this);

            return true;
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

            public ScaleMonitor(ScalingMonitor scalingMonitor, string uniqueIdentifier)
            {
                this.scalingMonitor = scalingMonitor;
                string descriptorString = $"DurableTaskTrigger-Netherite-{uniqueIdentifier}".ToLower();   
                this.descriptor = new ScaleMonitorDescriptor(descriptorString);
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
                    this.scalingMonitor.InformationTracer?.Invoke($"ScaleMonitor returned metrics cached previously, at {cached.Item2.Timestamp:o}");
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

                    this.scalingMonitor.InformationTracer?.Invoke(
                        $"ScaleMonitor collected metrics for {collectedMetrics.LoadInformation.Count} partitions at {collectedMetrics.Timestamp:o} in {sw.Elapsed.TotalMilliseconds:F2}ms.");
                }
                catch (Exception e)
                {
                    this.scalingMonitor.ErrorTracer?.Invoke("ScaleMonitor failed to collect metrics", e);
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
                    var lastMetric = metrics?.LastOrDefault(m => m?.Metrics != null);

                    if (lastMetric == null)
                    {
                        recommendation = new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: "missing metrics");
                    }
                    else
                    {
                        var stream = new MemoryStream(lastMetric.Metrics);
                        var collectedMetrics = (ScalingMonitor.Metrics) this.serializer.ReadObject(stream);                 
                        recommendation = this.scalingMonitor.GetScaleRecommendation(workerCount, collectedMetrics);
                    }
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    this.scalingMonitor.ErrorTracer?.Invoke("ScaleMonitor failed to compute scale recommendation", e);
                    recommendation = new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: "unexpected error");
                }
               
                this.scalingMonitor.RecommendationTracer?.Invoke(recommendation.Action.ToString(), workerCount, recommendation.Reason);

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

                return scaleStatus;
            }
        }
    }
}
#endif