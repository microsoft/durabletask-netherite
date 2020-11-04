// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Scaling
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.EventHubs;

    /// <summary>
    /// Monitors the performance of the Netherite backend and makes scaling decisions.
    /// </summary>
    public class ScalingMonitor
    {
        readonly string storageConnectionString;
        readonly string eventHubsConnectionString;
        readonly string partitionLoadTableName;
        readonly string taskHubName;
        readonly TransportConnectionString.TransportChoices configuredTransport;

        readonly AzureLoadMonitorTable table;

        /// <summary>
        /// Creates an instance of the scaling monitor, with the given parameters.
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string.</param>
        /// <param name="eventHubsConnectionString">The connection string for the transport layer.</param>
        /// <param name="partitionLoadTableName">The name of the storage table with the partition load information.</param>
        /// <param name="taskHubName">The name of the taskhub.</param>
        public ScalingMonitor(string storageConnectionString, string eventHubsConnectionString, string partitionLoadTableName, string taskHubName)
        {
            this.storageConnectionString = storageConnectionString;
            this.eventHubsConnectionString = eventHubsConnectionString;
            this.partitionLoadTableName = partitionLoadTableName;
            this.taskHubName = taskHubName;

            TransportConnectionString.Parse(eventHubsConnectionString, out _, out this.configuredTransport, out _);

            this.table = new AzureLoadMonitorTable(storageConnectionString, partitionLoadTableName, taskHubName);
        }

        /// <summary>
        /// Makes a scale recommendation.
        /// </summary>
        /// <returns></returns>
        public async Task<ScaleRecommendation> GetScaleRecommendation(int workerCount)
        {
            Dictionary<uint, PartitionLoadInfo> loadInformation = await this.table.QueryAsync(CancellationToken.None).ConfigureAwait(false);

            bool taskHubIsIdle = await this.TaskHubIsIdleAsync(loadInformation).ConfigureAwait(false);

            if (workerCount == 0 && !taskHubIsIdle)
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: "First worker");
            }

            if (loadInformation.Values.Any(partitionLoadInfo => partitionLoadInfo.LatencyTrend.Length < PartitionLoadInfo.LatencyTrendLength))
            {
                return new ScaleRecommendation(ScaleAction.None, keepWorkersAlive: !taskHubIsIdle, reason: "Not enough samples");
            }

            if (taskHubIsIdle)
            {
                return new ScaleRecommendation(
                    scaleAction: workerCount > 0 ? ScaleAction.RemoveWorker : ScaleAction.None,
                    keepWorkersAlive: false,
                    reason: "Task hub is idle");
            }

            int numberOfSlowPartitions = loadInformation.Values.Count(info => info.LatencyTrend.Last() == PartitionLoadInfo.HighLatency);

            if (workerCount < numberOfSlowPartitions)
            {
                // Some partitions are busy, so scale out until workerCount == partitionCount.
                var partition = loadInformation.First(kvp => kvp.Value.LatencyTrend.Last() == PartitionLoadInfo.HighLatency);
                return new ScaleRecommendation(
                    ScaleAction.AddWorker,
                    keepWorkersAlive: true,
                    reason: $"High latency in partition {partition.Key}: {partition.Value.LatencyTrend}");
            }

            int numberOfNonIdlePartitions = loadInformation.Values.Count(info => info.LatencyTrend.Any(c => c != PartitionLoadInfo.Idle));

            if (workerCount > numberOfNonIdlePartitions)
            {
                // If the work item queues are idle, scale down to the number of non-idle control queues.
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
            if (workerCount > 1 && (new Random()).Next(10) == 0)
            {
                bool allPartitionsAreFast = !loadInformation.Values.Any(
                    info => info.LatencyTrend.Any(c => c == PartitionLoadInfo.MediumLatency || c == PartitionLoadInfo.HighLatency));

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

        async Task<bool> TaskHubIsIdleAsync(Dictionary<uint, PartitionLoadInfo> loadInformation)
        {
            // first, check if any of the partitions have queued work or are scheduled to wake up
            foreach (var p in loadInformation.Values)
            {
                if (p.Activities > 0 || p.WorkItems > 0 || p.Requests > 0 || p.Outbox > 0)
                {
                    return false;
                }

                if (p.Wakeup.HasValue && p.Wakeup.Value < DateTime.UtcNow + TimeSpan.FromSeconds(10))
                {
                    return false;
                }
            }

            // next, check if any of the entries are not current, in the sense that their input queue position
            // does not match the latest queue position

            long[] positions;

            switch (this.configuredTransport)
            {
                case TransportConnectionString.TransportChoices.EventHubs:
                    positions = await EventHubs.EventHubsConnections.GetQueuePositions(this.eventHubsConnectionString, EventHubsTransport.PartitionHubs).ConfigureAwait(false);
                    break;

                default:
                    return false;
            }

            for (uint i = 0; i < positions.Length; i++)
            {
                if (!loadInformation.TryGetValue(i, out var loadInfo))
                {
                    return false;
                }
                if (positions[i] > loadInfo.InputQueuePosition)
                {
                    return false;
                }
            }

            // we have concluded that there are no pending work items, timers, or unprocessed input queue entries
            return true;
        }  
    }
}