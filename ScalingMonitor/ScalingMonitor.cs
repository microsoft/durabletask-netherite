// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.ScalingLogic
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite;
    using DurableTask.Netherite.EventHubs;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

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

        readonly ILoadMonitorService loadMonitor;

        /// <summary>
        /// The name of the taskhub.
        /// </summary>
        public string TaskHubName => this.taskHubName;

        /// <summary>
        /// A logger for scaling events.
        /// </summary>
        public ILogger Logger { get; }

        /// <summary>
        /// Creates an instance of the scaling monitor, with the given parameters.
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string.</param>
        /// <param name="eventHubsConnectionString">The connection string for the transport layer.</param>
        /// <param name="partitionLoadTableName">The name of the storage table with the partition load information.</param>
        /// <param name="taskHubName">The name of the taskhub.</param>
        public ScalingMonitor(
            string storageConnectionString, 
            string eventHubsConnectionString, 
            string partitionLoadTableName, 
            string taskHubName,
            ILogger logger)
        {
            this.storageConnectionString = storageConnectionString;
            this.eventHubsConnectionString = eventHubsConnectionString;
            this.partitionLoadTableName = partitionLoadTableName;
            this.taskHubName = taskHubName;
            this.Logger = logger;

            TransportConnectionString.Parse(eventHubsConnectionString, out _, out this.configuredTransport);

            if (!string.IsNullOrEmpty(partitionLoadTableName))
            {
                this.loadMonitor = new AzureTableLoadMonitor(storageConnectionString, partitionLoadTableName, taskHubName);
            }
            else
            {
                this.loadMonitor = new AzureBlobLoadMonitor(storageConnectionString, taskHubName);
            }
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
            DateTime now = DateTime.UtcNow;
            var loadInformation = await this.loadMonitor.QueryAsync(CancellationToken.None).ConfigureAwait(false);
            var busy = await this.TaskHubIsIdleAsync(loadInformation).ConfigureAwait(false);

            return new Metrics()
            {
                LoadInformation = loadInformation,
                Busy = busy,
                Timestamp = now,
            };
        }

        /// <summary>
        /// Makes a scale recommendation.
        /// </summary>
        /// <returns></returns>
        public ScaleRecommendation GetScaleRecommendation(int workerCount, Metrics metrics)
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

            int numberOfSlowPartitions = metrics.LoadInformation.Values.Count(info => info.LatencyTrend.Length > 1 && info.LatencyTrend.Last() == PartitionLoadInfo.HighLatency);

            if (workerCount < numberOfSlowPartitions)
            {
                // scale up to the number of busy partitions
                var partition = metrics.LoadInformation.First(kvp => kvp.Value.LatencyTrend.Last() == PartitionLoadInfo.HighLatency);
                return new ScaleRecommendation(
                    ScaleAction.AddWorker,
                    keepWorkersAlive: true,
                    reason: $"High latency in partition {partition.Key}: {partition.Value.LatencyTrend}");
            }

            int numberOfNonIdlePartitions = metrics.LoadInformation.Values.Count(info => ! PartitionLoadInfo.IsLongIdle(info.LatencyTrend));

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

            long[] positions;
             string[] PartitionHubs = { "partitions" }; // EventHubsTransport.PartitionHubs (used in line 209)

            if (this.configuredTransport == TransportConnectionString.TransportChoices.EventHubs)
            {
                (positions, _, _) = await GetPartitionInfo(this.eventHubsConnectionString, PartitionHubs).ConfigureAwait(false);

                for (uint i = 0; i < positions.Length; i++)
                {
                    if (!loadInformation.TryGetValue(i, out var loadInfo))
                    {
                        return $"P{i:D2} has no load information published yet";
                    }
                    if (positions[i] > loadInfo.InputQueuePosition)
                    {
                        return $"P{i:D2} has input queue position {loadInfo.InputQueuePosition} which is {positions[i] - loadInfo.InputQueuePosition} behind latest position {positions[i]}";
                    }
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

        public static async Task<(long[], DateTime[], string)> GetPartitionInfo(string connectionString, string[] partitionHubs)
        {
            var connections = new EventHubsConnections(connectionString, partitionHubs, new string[0]);
            await connections.GetPartitionInformationAsync();

            var numberPartitions = connections.partitionPartitions.Count;

            var positions = new long[numberPartitions];

            var infoTasks = connections.partitionPartitions
                .Select(x => x.client.GetPartitionRuntimeInformationAsync(x.id)).ToList();

            await Task.WhenAll(infoTasks);

            for (int i = 0; i < numberPartitions; i++)
            {
                var queueInfo = await infoTasks[i].ConfigureAwait(false);
                positions[i] = queueInfo.LastEnqueuedSequenceNumber + 1;
            }

            await connections.StopAsync();

            return (positions, connections.CreationTimestamps, connections.Endpoint);
        }
    }

    public class EventHubsConnections
    {
        readonly string connectionString;
        readonly string[] partitionHubs;
        readonly string[] clientHubs;

        List<EventHubClient> partitionClients;

        public readonly List<(EventHubClient client, string id)> partitionPartitions = new List<(EventHubClient client, string id)>();

        public string Endpoint { get; private set; }

        public DateTime[] CreationTimestamps { get; private set; }

        public EventHubsConnections(
            string connectionString,
            string[] partitionHubs,
            string[] clientHubs)
        {
            this.connectionString = connectionString;
            this.partitionHubs = partitionHubs;
            this.clientHubs = clientHubs;
        }

        public async Task GetPartitionInformationAsync()
        {
            // create partition clients
            this.partitionClients = new List<EventHubClient>();
            for (int i = 0; i < this.partitionHubs.Length; i++)
            {
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
                {
                    EntityPath = this.partitionHubs[i]
                };
                var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                this.partitionClients.Add(client);
                this.Endpoint = connectionStringBuilder.Endpoint.ToString();
            }

            // in parallel, get runtime infos for all the hubs
            var partitionInfos = this.partitionClients.Select((ehClient) => ehClient.GetRuntimeInformationAsync()).ToList();
            await Task.WhenAll(partitionInfos);

            this.CreationTimestamps = partitionInfos.Select(t => t.Result.CreatedAt).ToArray();

            // create a flat list of partition partitions
            for (int i = 0; i < this.partitionHubs.Length; i++)
            {
                foreach (var id in partitionInfos[i].Result.PartitionIds)
                {
                    this.partitionPartitions.Add((this.partitionClients[i], id));
                }
            }
        }

        public async Task StopAsync()
        {
            IEnumerable<EventHubClient> Clients()
            {
                if (this.partitionClients != null)
                {
                    foreach (var client in this.partitionClients)
                    {
                        yield return client;
                    }
                }
            }

            await Task.WhenAll(Clients().Select(client => client.CloseAsync()).ToList());
        }
    }
}