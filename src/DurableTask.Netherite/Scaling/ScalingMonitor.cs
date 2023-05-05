// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.EventHubsTransport;

    /// <summary>
    /// Monitors the performance of the Netherite backend and makes scaling decisions.
    /// </summary>
    public class ScalingMonitor
    {
        readonly ConnectionInfo eventHubsConnection;
        readonly string partitionLoadTableName;
        readonly string taskHubName;
        readonly ILoadPublisherService loadPublisher;
        readonly NetheriteMetricsProvider netheriteMetricsProvider;

        // public logging actions to enable collection of scale-monitor-related logging within the Netherite infrastructure
        public Action<string, int, string> RecommendationTracer { get; }
        public Action<string> InformationTracer { get; }
        public Action<string, Exception> ErrorTracer { get; }

        /// <summary>
        /// The name of the taskhub.
        /// </summary>
        public string TaskHubName => this.taskHubName;

        /// <summary>
        /// Creates an instance of the scaling monitor, with the given parameters.
        /// </summary>
        /// <param name="storageConnection">The storage connection info.</param>
        /// <param name="eventHubsConnection">The connection info for event hubs.</param>
        /// <param name="partitionLoadTableName">The name of the storage table with the partition load information.</param>
        /// <param name="taskHubName">The name of the taskhub.</param>
        public ScalingMonitor(
            ILoadPublisherService loadPublisher,
            ConnectionInfo eventHubsConnection,
            string partitionLoadTableName,
            string taskHubName,
            Action<string, int, string> recommendationTracer,
            Action<string> informationTracer,
            Action<string, Exception> errorTracer,
            NetheriteMetricsProvider netheriteMetricsProvider)
        {
            this.RecommendationTracer = recommendationTracer;
            this.InformationTracer = informationTracer;
            this.ErrorTracer = errorTracer;

            this.loadPublisher = loadPublisher;
            this.eventHubsConnection = eventHubsConnection;
            this.partitionLoadTableName = partitionLoadTableName;
            this.taskHubName = taskHubName;

            this.netheriteMetricsProvider = netheriteMetricsProvider;
        }

        /// <summary>
        /// The metrics that are collected prior to making a scaling decision
        /// </summary>
        [DataContract]
        public struct Metrics
        {
            /// <summary>
            ///  the most recent load information published for each partition
            /// </summary>
            [DataMember]
            public Dictionary<uint, PartitionLoadInfo> LoadInformation { get; set; }

            /// <summary>
            /// A reason why the taskhub is not idle, or null if it is idle
            /// </summary>
            [DataMember]
            public string Busy { get; set; }

            /// <summary>
            /// Whether the taskhub is idle
            /// </summary>
            [IgnoreDataMember]
            public bool TaskHubIsIdle => string.IsNullOrEmpty(this.Busy);

            /// <summary>
            /// The time at which the metrics were collected
            /// </summary>
            [DataMember]
            public DateTime Timestamp { get; set; }
        }

        /// <summary>
        /// Collect the metrics for making the scaling decision.
        /// </summary>
        /// <returns>The collected metrics.</returns>
        public async Task<Metrics> CollectMetrics()
        {
            return await this.netheriteMetricsProvider.GetMetricsAsync();
        }

        /// <summary>
        /// Makes a scale recommendation.
        /// </summary>
        /// <returns></returns>
        public ScaleRecommendation GetScaleRecommendation(int workerCount, Metrics metrics)
        {
            var recommendation = DetermineRecommendation();

            return recommendation;

            ScaleRecommendation DetermineRecommendation()
            {
                if (workerCount == 0 && !metrics.TaskHubIsIdle)
                {
                    return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: metrics.Busy);
                }

                if (metrics.TaskHubIsIdle)
                {
                    return new ScaleRecommendation(
                        scaleAction: workerCount > 0 ? ScaleAction.RemoveWorker : ScaleAction.None,
                        keepWorkersAlive: false,
                        reason: "Task hub is idle");
                }

                bool isSlowPartition(PartitionLoadInfo info)
                {
                    char mostRecent = info.LatencyTrend.Last();
                    return mostRecent == PartitionLoadInfo.HighLatency || mostRecent == PartitionLoadInfo.MediumLatency;
                }
                int numberOfSlowPartitions = metrics.LoadInformation.Values.Count(info => isSlowPartition(info));

                if (workerCount < numberOfSlowPartitions)
                {
                    // scale up to the number of busy partitions
                    return new ScaleRecommendation(
                        ScaleAction.AddWorker,
                        keepWorkersAlive: true,
                        reason: $"Significant latency in {numberOfSlowPartitions} partitions");
                }

                int backlog = metrics.LoadInformation.Where(info => info.Value.IsLoaded()).Sum(info => info.Value.Activities);

                if (backlog > 0 && workerCount < metrics.LoadInformation.Count)
                {
                    return new ScaleRecommendation(
                        ScaleAction.AddWorker,
                        keepWorkersAlive: true,
                        reason: $"Backlog of {backlog} activities");
                }

                int numberOfNonIdlePartitions = metrics.LoadInformation.Values.Count(info => !PartitionLoadInfo.IsLongIdle(info.LatencyTrend));

                if (workerCount > numberOfNonIdlePartitions)
                {
                    // scale down to the number of non-idle partitions.
                    return new ScaleRecommendation(
                        ScaleAction.RemoveWorker,
                        keepWorkersAlive: true,
                        reason: $"One or more partitions are idle");
                }

                // If all queues are operating efficiently, it can be hard to know if we need to reduce the worker count.
                // We want to avoid the case where a constant trickle of load after a big scale-out prevents scaling back in.
                // We also want to avoid scaling in unnecessarily when we've reached optimal scale-out. To balance these
                // goals, we check for low latencies and vote to scale down 10% of the time when we see this. The thought is
                // that it's a slow scale-in that will get automatically corrected once latencies start increasing again.
                if (workerCount > 1 && (new Random()).Next(8) == 0)
                {
                    bool allPartitionsAreFast = !metrics.LoadInformation.Values.Any(
                        info => info.LatencyTrend.Length == PartitionLoadInfo.LatencyTrendLength
                            && info.LatencyTrend.Any(c => c == PartitionLoadInfo.MediumLatency || c == PartitionLoadInfo.HighLatency));

                    if (allPartitionsAreFast)
                    {
                        return new ScaleRecommendation(
                               ScaleAction.RemoveWorker,
                               keepWorkersAlive: true,
                               reason: $"All partitions are fast");
                    }
                }

                // Load exists, but none of our scale filters were triggered, so we assume that the current worker
                // assignments are close to ideal for the current workload.
                return new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: true, reason: $"Partition latencies are healthy");
            }
        }

        /// <summary>
        /// Makes a target recommendation.
        /// </summary>
        /// <returns>The recommended number of workers.</returns>
        public int GetTargetRecommendation(Metrics metrics, int maxConcurrentActivities, int maxConcurrentWorkItems)
        {
            if (metrics.TaskHubIsIdle)
            {
                return 0; // we need no workers
            }

            int target = 1; // always need at least one worker when we are not idle

            // if there is a backlog of activities, ask for enough workers to process them
            int activities = metrics.LoadInformation.Where(info => info.Value.IsLoaded()).Sum(info => info.Value.Activities);
            if (activities > 0)
            {
                int requestedWorkers = (activities / maxConcurrentActivities) + 1;
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
                    bool allPartitionsAreFast = !metrics.LoadInformation.Values.Any( info => 
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

            return target;
        }
    }
}
