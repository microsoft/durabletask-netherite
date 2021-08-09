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

        const string OFFLOAD_METRIC = "Load";
        const int OFFLOAD_MAX_BATCH_SIZE = 20;
        const int OFFLOAD_MIN_BATCH_SIZE = 10;
        const int OVERLOAD_THRESHOLD = 20;
        const int UNDERLOAD_THRESHOLD = 5;

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
                $"mobile={loadInformationReceived.Mobile} stationary={loadInformationReceived.Stationary} and avg completion time={loadInformationReceived.AverageActCompletionTime}");

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
                    .Select(t => t.Value.AverageActCompletionTime).Average() ?? 100;
                double EstimatedActCompletionTime(uint partitionId) 
                    => this.LoadInfo[partitionId].AverageActCompletionTime ?? DefaultEstimatedCompletionTime;

                this.MakeDecisions(loadInformationReceived.PartitionId, EstimatedActCompletionTime);
            }
            catch (Exception e)
            {
                this.traceHelper.TraceError($"Caught exception: ", e);
            }
        }

        void MakeDecisions(uint overloadCandidate, Func<uint,double> EstimatedActCompletionTime)
        {
            string estimated = string.Join(',', this.LoadInfo.Values.Select(i => i.EstimatedLoad.ToString()));
            string mobile = string.Join(',', this.LoadInfo.Values.Select(i => i.EstimatedMobile.ToString()));
            this.traceHelper.TraceWarning($"Decision pending={this.PendingOnSource.Count},{this.PendingOnDestination.Count} estimated=[{estimated}] mobile=[{mobile}]");

            if (this.LoadInfo.Values.Select(i => i.EstimatedMobile).Sum() == 0)
            {
                // there are no mobile activities, so there is no rebalancing possible
                return;
            }

            // check if it makes sense to redistribute work from this partition to other partitions
            double AverageWaitTime = this.LoadInfo.Average(t => t.Value.EstimatedLoad * EstimatedActCompletionTime(t.Key));
            double CompletionTime = this.LoadInfo[overloadCandidate].EstimatedLoad * EstimatedActCompletionTime(overloadCandidate);

            // if the expected  completion time is above average, we consider redistributing the load
            if (CompletionTime > AverageWaitTime)
            {
                // start the task migration process and find offload targets
                //if (this.FindOffloadTargetByLoad(loadInformationReceived.PartitionId, loadInformationReceived.BacklogSize - OVERLOAD_THRESHOLD, out Dictionary<uint, int> OffloadTargets))
                if (FindOffloadTargetByWaitTime(this.LoadInfo[overloadCandidate].EstimatedMobile, out Dictionary<uint, int> OffloadTargets))
                {
                    // send offload commands
                    foreach (var kvp in OffloadTargets)
                    {
                        this.SendOffloadCommand(overloadCandidate, kvp.Key, kvp.Value);
                    }
                }
            }

            bool FindOffloadTargetByWaitTime(int numActivitiesToOffload, out Dictionary<uint, int> OffloadTargets)
            {
                OffloadTargets = new Dictionary<uint, int>();
                for (uint i = 0; i < this.host.NumberPartitions - 1 && numActivitiesToOffload > 0; i++)
                {
                    uint target = (overloadCandidate + i + 1) % this.host.NumberPartitions;

                    if (this.LoadInfo.TryGetValue(target, out var targetInfo))
                    {
                        int portionSize = Math.Min(OFFLOAD_MAX_BATCH_SIZE, numActivitiesToOffload);

                        double localWaitTime = (this.LoadInfo[overloadCandidate].EstimatedLoad - portionSize) * EstimatedActCompletionTime(overloadCandidate);
                        double remoteWaitTime = targetInfo.EstimatedLoad * EstimatedActCompletionTime(target);

                        if (localWaitTime > remoteWaitTime)
                        {
                            OffloadTargets[target] = portionSize;
                            numActivitiesToOffload -= portionSize;
                        }
                    }
                }

                return OffloadTargets.Count == 0 ? false : true;
            }
        }

        bool FindOffloadTargetByLoad(uint currentPartitionId, int numActivitiesToOffload, out Dictionary<uint, int> OffloadTargets)
        {
            OffloadTargets = new Dictionary<uint, int>();
            for (uint i = 0; i < this.host.NumberPartitions - 1 && numActivitiesToOffload > 0; i++)
            {
                uint target = (currentPartitionId + i + 1) % this.host.NumberPartitions;
                if (this.LoadInfo[target].EstimatedLoad < UNDERLOAD_THRESHOLD)
                {
                    int capacity = OVERLOAD_THRESHOLD - this.LoadInfo[target].EstimatedLoad;
                    OffloadTargets[target] = numActivitiesToOffload - capacity > 0 ? capacity : numActivitiesToOffload;
                    numActivitiesToOffload -= capacity;

                    // prevent the load monitor offload more tasks before the target partition reports its current load
                    // this.LoadInfo[target] = OVERLOAD_THRESHOLD;
                }
            }
            return OffloadTargets.Count == 0 ? false : true;
        }
    }
}
