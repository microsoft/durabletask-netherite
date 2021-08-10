// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;

    class LoadMonitor : TransportAbstraction.ILoadMonitor
    {
        readonly NetheriteOrchestrationService host;
        readonly LoadMonitorTraceHelper traceHelper;

        TransportAbstraction.ISender BatchSender { get; set; }

        // data structure for the load monitor (partition id -> (backlog size, average activity completion time))
        SortedDictionary<uint, Info> LoadInfo { get; set; }

        // the list of offload commands that are not yet acknowledged by the source partition
        List<OffloadCommandReceived> PendingOnSource { get; set; }

        // the list of offload commands that are not yet acknowledged by the destination partition
        List<OffloadCommandReceived> PendingOnDestination { get; set; }

        // estimated communication delay between partitions and the load monitor
        public double ESTIMATED_RTT_MS { get; private set; } = 20000;

        public static string GetShortId(Guid clientId) => clientId.ToString("N").Substring(0, 7);

        class Info
        {
            // latest metrics as reported
            public int ReportedStationary;
            public int ReportedMobile;
            public double? AverageActCompletionTime;

            // estimations that take into account pending offload commands
            public int EstimatedStationary;
            public int EstimatedMobile;
            public int EstimatedLoad => this.EstimatedStationary + this.EstimatedMobile;
        }

        public LoadMonitor(
            NetheriteOrchestrationService host,
            Guid taskHubGuid,
            TransportAbstraction.ISender batchSender)
        {
            this.host = host;
            this.traceHelper = new LoadMonitorTraceHelper(host.Logger, host.Settings.LogLevelLimit, host.StorageAccountName, host.Settings.HubName);
            this.BatchSender = batchSender;
            this.LoadInfo = new SortedDictionary<uint, Info>();
            this.PendingOnSource = new List<OffloadCommandReceived>();
            this.PendingOnDestination = new List<OffloadCommandReceived>();
            this.traceHelper.TraceWarning("Started");
        }

        long lastTimestamp = long.MinValue;
        DateTime GetUniqueTimestamp()
        {
            var ts = Math.Max(this.lastTimestamp + 1, DateTime.UtcNow.Ticks); // hybrid clock
            this.lastTimestamp = ts;
            return new DateTime(ts, DateTimeKind.Utc);
        }

        public Task StopAsync()
        {
            this.traceHelper.TraceWarning("Stopped");
            return Task.CompletedTask;
        }

        public void ReportTransportError(string message, Exception e)
        {
            this.traceHelper.TraceError($"Error reported by transport: {message}", e);
        }

        void TransportAbstraction.ILoadMonitor.Process(LoadMonitorEvent loadMonitorEvent)
        {
            // dispatch call to matching method
            this.Process((dynamic)loadMonitorEvent);
        }

        void SendOffloadCommand(uint from, uint to, int num)
        {
            var offloadCommand = new OffloadCommandReceived()
            {
                RequestId = Guid.NewGuid(),
                PartitionId = from,
                NumActivitiesToSend = num,
                OffloadDestination = to,
                Timestamp = this.GetUniqueTimestamp(),
            };

            this.traceHelper.TraceWarning($"Sending offloadCommand: move {num} from {from} to {to} (id={offloadCommand.Timestamp:o})");

            this.PendingOnSource.Add(offloadCommand);
            this.PendingOnDestination.Add(offloadCommand);
            this.BatchSender.Submit(offloadCommand);
        }

        void ComputeBacklogEstimates()
        {
            // start from the reported baseline
            foreach(var info in this.LoadInfo.Values)
            {
                info.EstimatedStationary = info.ReportedStationary;
                info.EstimatedMobile = info.ReportedMobile;
            }
            // then add the effect of all pending commands
            foreach (var command in this.PendingOnDestination)
            {
                this.LoadInfo[command.OffloadDestination].EstimatedStationary += command.NumActivitiesToSend;
            }
            foreach (var command in this.PendingOnSource)
            {
                int estimate = this.LoadInfo[command.PartitionId].EstimatedMobile;
                estimate = Math.Max(0, estimate - command.NumActivitiesToSend);
                this.LoadInfo[command.PartitionId].EstimatedMobile = estimate;
            }
        }

        public void Process(LoadInformationReceived loadInformationReceived)
        {
            this.traceHelper.TraceWarning($"Received Load information from partition{loadInformationReceived.PartitionId} with " +
                $"mobile={loadInformationReceived.Mobile} stationary={loadInformationReceived.Stationary} and AverageActCompletionTime={loadInformationReceived.AverageActCompletionTime}");

            try
            {
                // update load info
                if (!this.LoadInfo.TryGetValue(loadInformationReceived.PartitionId, out var info))
                {
                    info = this.LoadInfo[loadInformationReceived.PartitionId] = new Info();
                }
                info.ReportedMobile = loadInformationReceived.Mobile;
                info.ReportedStationary = loadInformationReceived.Stationary;
                info.AverageActCompletionTime = loadInformationReceived.AverageActCompletionTime;

                // process acks
                this.PendingOnSource = this.PendingOnSource.Where(c => !loadInformationReceived.ConfirmsSource(c)).ToList();
                this.PendingOnDestination = this.PendingOnDestination.Where(c => !loadInformationReceived.ConfirmsDestination(c)).ToList();

                // if we don't have information for all partitions yet, do not continue
                if (this.LoadInfo.Count < this.host.NumberPartitions)
                {
                    return;
                }

                // compute estimated backlog sizes based on reported size and pending commands
                this.ComputeBacklogEstimates();

                // create a default estimation of completion time (we use this to replace missing estimates)
                double DefaultEstimatedCompletionTime = this.LoadInfo
                    .Select(t => t.Value.AverageActCompletionTime).Average() ?? TimeSpan.FromMinutes(1).TotalMilliseconds;
                double EstimatedActCompletionTime(uint partitionId) 
                    => this.LoadInfo[partitionId].AverageActCompletionTime ?? DefaultEstimatedCompletionTime;

                this.MakeDecisions(loadInformationReceived.PartitionId, EstimatedActCompletionTime);
            }
            catch (Exception e)
            {
                this.traceHelper.TraceError($"Caught exception: ", e);
            }
        }

        void MakeDecisions(uint underloadCandidate, Func<uint, double> EstimatedActCompletionTime)
        {
            string estimated = string.Join(',', this.LoadInfo.Values.Select(i => i.EstimatedLoad.ToString()));
            string mobile = string.Join(',', this.LoadInfo.Values.Select(i => i.EstimatedMobile.ToString()));
            this.traceHelper.TraceWarning($"Decision pending={this.PendingOnSource.Count},{this.PendingOnDestination.Count} estimated=[{estimated}] mobile=[{mobile}]");

            if (this.LoadInfo.Values.Select(i => i.EstimatedMobile).Sum() == 0)
            {
                // there are no mobile activities, so there is no rebalancing possible
                return;
            }

            // get the current and target queue length
            int currentQueueLen = this.LoadInfo[underloadCandidate].EstimatedLoad;
            int targetQueueLen = (int)Math.Ceiling(this.ESTIMATED_RTT_MS * this.host.Settings.MaxConcurrentActivityFunctions / EstimatedActCompletionTime(underloadCandidate));
            this.traceHelper.TraceWarning($"Partition={underloadCandidate}, ESTIMATED_RTT_MS={this.ESTIMATED_RTT_MS}, EstimatedActCompletionTime={EstimatedActCompletionTime(underloadCandidate)}, currentQueueLength={currentQueueLen}, and targetQueueLength={targetQueueLen}");


            // if the current queue length is smaller than the desired queue length, try to pull works from other partitions
            if (currentQueueLen < targetQueueLen)
            {
                if (tryPullActFromRemote(underloadCandidate, targetQueueLen - currentQueueLen, out Dictionary<uint, int> OffloadTargets))
                {
                    // send offload commands
                    foreach (var kvp in OffloadTargets)
                    {
                        this.SendOffloadCommand(kvp.Key, underloadCandidate, kvp.Value);
                    }
                }
            }

            bool tryPullActFromRemote(uint underloadCandidate, int numActToPull, out Dictionary<uint, int> OffloadTargets)
            {
                OffloadTargets = new Dictionary<uint, int>();

                for (uint i = 0; i < this.host.NumberPartitions - 1 && numActToPull > 0; i++)
                {
                    uint target = (underloadCandidate + i + 1) % this.host.NumberPartitions;
                    int targetCurrentQueueLen = this.LoadInfo[target].EstimatedLoad;
                    int targetTargetQueueLen = (int)Math.Ceiling(this.ESTIMATED_RTT_MS * this.host.Settings.MaxConcurrentActivityFunctions / EstimatedActCompletionTime(target));

                    int availableToPull = Math.Min(targetCurrentQueueLen - targetTargetQueueLen, this.LoadInfo[target].EstimatedMobile);
                    int amount = Math.Min(availableToPull, numActToPull);

                    if (amount > 0)
                    {
                        numActToPull -= amount;
                        OffloadTargets[target] = amount;
                    }
                }

                return OffloadTargets.Count == 0 ? false : true;
            }
        }
    }
}
