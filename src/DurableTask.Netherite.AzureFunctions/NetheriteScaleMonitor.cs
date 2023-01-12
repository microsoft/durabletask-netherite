#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using static DurableTask.Netherite.AzureFunctions.NetheriteProvider;
    using System.Threading.Tasks;
    using System.Linq;

    /// <summary>
    /// Scale monitor for Durable Functions using Netherite backend.
    /// </summary>
    public class NetheriteScaleMonitor : IScaleMonitor<NetheriteScaleMetrics>
    {
        readonly ScalingMonitor scalingMonitor;
        readonly ScaleMonitorDescriptor descriptor;
        readonly DataContractSerializer serializer = new DataContractSerializer(typeof(ScalingMonitor.Metrics));
        static Tuple<DateTime, NetheriteScaleMetrics> cachedMetrics;

        /// <summary>
        /// Creates a new instance of the NetheriteScaleMonitor.
        /// </summary>
        /// <param name="scalingMonitor">Inner scaling monitor that the NetheriteScaleMonitor uses for scale decisions.</param>
        public NetheriteScaleMonitor(ScalingMonitor scalingMonitor)
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
                if (metrics == null || metrics.Length == 0)
                {
                    recommendation = new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: "missing metrics");
                }
                else
                {
                    var stream = new MemoryStream(metrics[metrics.Length - 1].Metrics);
                    var collectedMetrics = (ScalingMonitor.Metrics)this.serializer.ReadObject(stream);
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
#endif