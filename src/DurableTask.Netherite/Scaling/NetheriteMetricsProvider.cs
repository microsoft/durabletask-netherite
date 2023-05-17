// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.EventHubsTransport;
    using static DurableTask.Netherite.Scaling.ScalingMonitor;

    public class NetheriteMetricsProvider
    {
        readonly ILoadPublisherService loadPublisher;
        readonly ConnectionInfo eventHubsConnection;

        public NetheriteMetricsProvider(
            ILoadPublisherService loadPublisher,
            ConnectionInfo eventHubsConnection)
        {
            this.loadPublisher = loadPublisher;
            this.eventHubsConnection = eventHubsConnection;
        }

        public virtual async Task<Metrics> GetMetricsAsync()
        {
            DateTime now = DateTime.UtcNow;
            var loadInformation = await this.loadPublisher.QueryAsync(CancellationToken.None).ConfigureAwait(false);
            var busy = await this.TaskHubIsIdleAsync(loadInformation).ConfigureAwait(false);

            return new Metrics()
            {
                LoadInformation = loadInformation,
                Busy = busy,
                Timestamp = now,
            };
        }

        /// <summary>
        /// Determines if a taskhub is busy, based on load information for the partitions and on the eventhubs queue positions
        /// </summary>
        /// <param name="loadInformation"></param>
        /// <returns>null if the hub is idle, or a string describing the current non-idle state</returns>
        public async Task<string> TaskHubIsIdleAsync(Dictionary<uint, PartitionLoadInfo> loadInformation)
        {
            // first, check if any of the partitions have queued work or are scheduled to wake up
            foreach (var kvp in loadInformation)
            {
                string busy = kvp.Value.IsBusy();
                if (!string.IsNullOrEmpty(busy))
                {
                    return $"P{kvp.Key:D2} {busy}";
                }
            }

            // next, check if any of the entries are not current, in the sense that their input queue position
            // does not match the latest queue position


            List<long> positions = await Netherite.EventHubsTransport.EventHubsConnections.GetQueuePositionsAsync(this.eventHubsConnection, EventHubsTransport.PartitionHub).ConfigureAwait(false);

            if (positions == null)
            {
                return "eventhubs is missing";
            }

            for (int i = 0; i < positions.Count; i++)
            {
                if (!loadInformation.TryGetValue((uint)i, out var loadInfo))
                {
                    return $"P{i:D2} has no load information published yet";
                }
                if (positions[i] > loadInfo.InputQueuePosition)
                {
                    return $"P{i:D2} has input queue position {loadInfo.InputQueuePosition} which is {positions[(int)i] - loadInfo.InputQueuePosition} behind latest position {positions[i]}";
                }
            }

            // finally, check if we have waited long enough
            foreach (var kvp in loadInformation)
            {
                string latencyTrend = kvp.Value.LatencyTrend;

                if (!PartitionLoadInfo.IsLongIdle(latencyTrend))
                {
                    return $"P{kvp.Key:D2} had some activity recently, latency trend is {latencyTrend}";
                }
            }

            // we have concluded that there are no pending work items, timers, or unprocessed input queue entries
            return null;
        }
    }
}
