// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if !NETSTANDARD
#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using static DurableTask.Netherite.Scaling.ScalingMonitor;

    public class NetheriteTargetScaler : ITargetScaler
    {
        readonly NetheriteMetricsProvider metricsProvider;
        readonly DurabilityProvider durabilityProvider;
        readonly TargetScalerResult scaleResult;

        public NetheriteTargetScaler(
            string functionId,
            NetheriteMetricsProvider metricsProvider,
            DurabilityProvider durabilityProvider)
        {
            this.metricsProvider = metricsProvider;
            this.durabilityProvider = durabilityProvider;
            this.scaleResult = new TargetScalerResult();
            this.TargetScalerDescriptor = new TargetScalerDescriptor(functionId);
        }

        public TargetScalerDescriptor TargetScalerDescriptor { get; private set; }

        public async Task<TargetScalerResult> GetScaleResultAsync(TargetScalerContext context)
        {
            Metrics metrics = await this.metricsProvider.GetMetricsAsync();

            int maxConcurrentActivities = this.durabilityProvider.MaxConcurrentTaskActivityWorkItems;
            int maxConcurrentWorkItems = this.durabilityProvider.MaxConcurrentTaskOrchestrationWorkItems;

            int target;

            if (metrics.TaskHubIsIdle)
            {
                this.scaleResult.TargetWorkerCount = 0; // we need no workers
                return this.scaleResult;
            }

            target = 1; // always need at least one worker when we are not idle

            // if there is a backlog of activities, ask for enough workers to process them
            int activities = metrics.LoadInformation.Where(info => info.Value.IsLoaded()).Sum(info => info.Value.Activities);
            if (activities > 0)
            {
                int requestedWorkers = (activities + (maxConcurrentActivities - 1)) / maxConcurrentActivities; // rounded-up integer division
                requestedWorkers = Math.Min(requestedWorkers, metrics.LoadInformation.Count); // cannot use more workers than partitions
                target = Math.Max(target, requestedWorkers);
            }

            // if there are load-challenged partitions, ask for a worker for each of them
            int numberOfChallengedPartitions = metrics.LoadInformation.Values
                .Count(info => info.IsLoaded() || info.WorkItems > maxConcurrentWorkItems);
            target = Math.Max(target, numberOfChallengedPartitions);

            // Determine how many different workers are currently running
            int current = metrics.LoadInformation.Values.Select(info => info.WorkerId).Distinct().Count();

            if (target < current)
            {
                // the target is lower than our current scale. However, before
                // scaling in, we check some more things to avoid
                // over-aggressive scale-in that could impact performance negatively.

                int numberOfNonIdlePartitions = metrics.LoadInformation.Values.Count(info => !PartitionLoadInfo.IsLongIdle(info.LatencyTrend));
                if (current > numberOfNonIdlePartitions)
                {
                    // if we have more workers than non-idle partitions, don't immediately go lower than
                    // the number of non-idle partitions.
                    target = Math.Max(target, numberOfNonIdlePartitions);
                }
                else
                {
                    // All partitions are busy, so so we don't want to reduce the worker count unless load is very low.
                    // Even if all partitions are runnning efficiently, it can be hard to know whether it is wise to reduce the worker count.
                    // We want to avoid scaling in unnecessarily when we've reached optimal scale-out. 
                    // But we also want to avoid the case where a constant trickle of load after a big scale-out prevents scaling back in.
                    // To balance these goals, we vote to scale down only by one worker at a time when we see this situation.
                    bool allPartitionsAreFast = !metrics.LoadInformation.Values.Any(info =>
                              info.LatencyTrend.Length != PartitionLoadInfo.LatencyTrendLength
                           || info.LatencyTrend.Any(c => c == PartitionLoadInfo.MediumLatency || c == PartitionLoadInfo.HighLatency));

                    if (allPartitionsAreFast)
                    {
                        // don't go lower than 1 below current
                        target = Math.Max(target, current - 1);
                    }
                    else
                    {
                        // don't go lower than current
                        target = Math.Max(target, current);
                    }
                }
            }

            this.scaleResult.TargetWorkerCount = target;
            return this.scaleResult;
        }
    }
}
#endif
#endif