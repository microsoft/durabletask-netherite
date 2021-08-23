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

        // the list of transfer commands that are not yet acknowledged by the source partition
        List<TransferCommandReceived> PendingOnSource { get; set; }

        // the list of transfer commands that are not yet acknowledged by the destination partition
        List<TransferCommandReceived> PendingOnDestination { get; set; }

        DateTime LastSolicitation { get; set; } = DateTime.MinValue;

        // estimated round-trip duration for transfer commands
        public double EstimatedTransferRTT = INITIAL_RTT_ESTIMATE.TotalMilliseconds;

        public static TimeSpan INITIAL_RTT_ESTIMATE = TimeSpan.FromSeconds(3);
        public static TimeSpan EXTRA_BUFFER_RESERVE = TimeSpan.FromSeconds(5);
        public const double SMOOTHING_FACTOR = 0.1;

        public static TimeSpan SOLICITATION_INTERVAL = TimeSpan.FromSeconds(10);
        public static TimeSpan SOLICITATION_VALIDITY = TimeSpan.FromSeconds(30);

        public static string GetShortId(Guid clientId) => clientId.ToString("N").Substring(0, 7);

        class Info
        {
            // latest metrics as reported
            public int ReportedStationary;
            public int ReportedMobile;
            public double? AverageActCompletionTime;

            // estimations that take into account pending transfer commands
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
            this.PendingOnSource = new List<TransferCommandReceived>();
            this.PendingOnDestination = new List<TransferCommandReceived>();
            this.traceHelper.TraceProgress("Started");
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
            this.traceHelper.TraceProgress("Stopped");
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

        void SendTransferCommand(uint from, uint to, int num)
        {
            var transferCommand = new TransferCommandReceived()
            {
                RequestId = Guid.NewGuid(),
                PartitionId = from,
                NumActivitiesToSend = num,
                TransferDestination = to,
                Timestamp = this.GetUniqueTimestamp(),
            };

            this.traceHelper.TraceProgress($"Sending transferCommand: move {num} from {from} to {to} (id={transferCommand.Timestamp:o})");

            this.PendingOnSource.Add(transferCommand);
            this.PendingOnDestination.Add(transferCommand);
            this.BatchSender.Submit(transferCommand);
        }

        void SendSolicitations()
        {
            DateTime timestamp = DateTime.UtcNow;

            // limit solicitation frequency
            if (timestamp - this.LastSolicitation < SOLICITATION_INTERVAL)
            {
                this.traceHelper.TraceProgress($"Sending solicitations, timestamp={timestamp:o}");

                for (uint partition = 0; partition < this.host.NumberPartitions; partition++)
                {
                    var solicitation = new SolicitationReceived()
                    {
                        RequestId = Guid.NewGuid(),
                        PartitionId = partition,
                        Timestamp = timestamp,
                    };
                    this.BatchSender.Submit(solicitation);
                }
            }
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
                this.LoadInfo[command.TransferDestination].EstimatedStationary += command.NumActivitiesToSend;
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

                // compute RTT for confirmations
                var transferRTT = this.PendingOnDestination
                    .Where(c => loadInformationReceived.ConfirmsDestination(c))
                    .Select(c => (double?)(DateTime.UtcNow - c.Timestamp).TotalMilliseconds)
                    .Average();

                // update estimated RTT using exponential average
                if (transferRTT.HasValue)
                {
                    this.EstimatedTransferRTT = SMOOTHING_FACTOR * transferRTT.Value + (1 - SMOOTHING_FACTOR) * this.EstimatedTransferRTT;
                }

                this.traceHelper.TraceProgress($"Received Load information from partition{loadInformationReceived.PartitionId} with " +
                    $"mobile={loadInformationReceived.Mobile} stationary={loadInformationReceived.Stationary} and AverageActCompletionTime={loadInformationReceived.AverageActCompletionTime} RTT={transferRTT}");

                // process acks
                this.PendingOnSource = this.PendingOnSource.Where(c => !loadInformationReceived.ConfirmsSource(c)).ToList();
                this.PendingOnDestination = this.PendingOnDestination.Where(c => !loadInformationReceived.ConfirmsDestination(c)).ToList();

                // if the reporting node is above TQS and can offload, solicit load reports from all partitions
                if (info.EstimatedMobile > 0 &&
                    info.AverageActCompletionTime.HasValue &&
                    this.TargetQueueLength(info.AverageActCompletionTime.Value) < info.EstimatedLoad)
                {
                    this.SendSolicitations();
                }

                // if we have load information from all partitions, try to see if we 
                // should transfer load to this node
                if (this.LoadInfo.Count == this.host.NumberPartitions)
                {
                    // compute estimated backlog sizes based on reported size and pending commands
                    this.ComputeBacklogEstimates();

                    // create a default estimation of average activity completion time
                    // (we use this to replace missing estimates)
                    double DefaultEstimatedCompletionTime = this.LoadInfo
                        .Select(t => t.Value.AverageActCompletionTime).Average() ?? TimeSpan.FromMinutes(1).TotalMilliseconds;
                    double EstimatedActCompletionTime(uint partitionId)
                        => this.LoadInfo[partitionId].AverageActCompletionTime ?? DefaultEstimatedCompletionTime;

                    this.ConsiderLoadTransfers(loadInformationReceived.PartitionId, EstimatedActCompletionTime);
                }
            }
            catch (Exception e)
            {
                this.traceHelper.TraceError($"Caught exception: ", e);
            }
        }

        int TargetQueueLength(double estimatedActCompletionTime) =>
            (int)Math.Ceiling(this.EstimatedTransferRTT * this.host.Settings.MaxConcurrentActivityFunctions / estimatedActCompletionTime);

        void ConsiderLoadTransfers(uint underloadCandidate, Func<uint, double> EstimatedActCompletionTime)
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
            int targetQueueLen = this.TargetQueueLength(EstimatedActCompletionTime(underloadCandidate));
            this.traceHelper.TraceProgress($"Partition={underloadCandidate}, ESTIMATED_RTT_MS={this.EstimatedTransferRTT}, EstimatedActCompletionTime={EstimatedActCompletionTime(underloadCandidate)}, currentQueueLength={currentQueueLen}, and targetQueueLength={targetQueueLen}");

            // if the current queue length is smaller than the desired queue length, try to pull works from other partitions
            if (currentQueueLen < targetQueueLen)
            {
                if (tryIssueTransferCommands(underloadCandidate, targetQueueLen - currentQueueLen, out Dictionary<uint, int> transferTargets))
                {
                    // send transfer commands
                    foreach (var kvp in transferTargets)
                    {
                        this.SendTransferCommand(kvp.Key, underloadCandidate, kvp.Value);
                    }
                }
            }

            bool tryIssueTransferCommands(uint underloadCandidate, int numActToPull, out Dictionary<uint, int> transferTargets)
            {
                transferTargets = new Dictionary<uint, int>();

                for (uint i = 0; i < this.host.NumberPartitions - 1 && numActToPull > 0; i++)
                {
                    uint target = (underloadCandidate + i + 1) % this.host.NumberPartitions;
                    int targetCurrentQueueLen = this.LoadInfo[target].EstimatedLoad;
                    int targetTargetQueueLen = this.TargetQueueLength(EstimatedActCompletionTime(target));

                    int availableToPull = Math.Min(
                        targetCurrentQueueLen - targetTargetQueueLen, 
                        this.LoadInfo[target].EstimatedMobile);

                    int amount = Math.Min(availableToPull, numActToPull);

                    if (amount > 0)
                    {
                        numActToPull -= amount;
                        transferTargets[target] = amount;
                    }
                }

                return transferTargets.Count == 0 ? false : true;
            }
        }
    }
}
