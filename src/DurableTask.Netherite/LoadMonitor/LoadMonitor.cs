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

        DateTime LastSolicitation { get; set; }

        // estimated round-trip duration for transfer commands
        public double EstimatedTransferRTT = INITIAL_RTT_ESTIMATE.TotalMilliseconds;

        public static TimeSpan INITIAL_RTT_ESTIMATE = TimeSpan.FromSeconds(3);
        public static TimeSpan EXTRA_BUFFER_RESERVE = TimeSpan.FromSeconds(5);
 
        public static TimeSpan MAX_PLANAHEAD_INTERVAL = TimeSpan.FromSeconds(60);

        public const double SMOOTHING_FACTOR = 0.1;

        public static TimeSpan SOLICITATION_INTERVAL = TimeSpan.FromSeconds(10);
        public static TimeSpan SOLICITATION_VALIDITY = TimeSpan.FromSeconds(30);

        public static TimeSpan COMMAND_EXPIRATION = TimeSpan.FromHours(1);

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

            public int ReportedLoad => this.ReportedStationary + this.ReportedMobile;
            public int ProjectedLoad => this.EstimatedStationary + this.EstimatedMobile;
        }

        public LoadMonitor(
            NetheriteOrchestrationService host,
            Guid taskHubGuid,
            TransportAbstraction.ISender batchSender)
        {
            this.host = host;
            this.traceHelper = new LoadMonitorTraceHelper(host.TraceHelper.Logger, host.Settings.LogLevelLimit, host.StorageAccountName, host.Settings.HubName);
            this.BatchSender = batchSender;
            this.LoadInfo = new SortedDictionary<uint, Info>();
            this.PendingOnSource = new List<TransferCommandReceived>();
            this.PendingOnDestination = new List<TransferCommandReceived>();
            this.LastSolicitation = DateTime.MinValue;
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

        void TransportAbstraction.ILoadMonitor.Process(LoadMonitorEvent loadMonitorEvent)
        {
            // dispatch call to matching method
            this.Process((dynamic)loadMonitorEvent);
        }

        void SendTransferCommand(uint from, uint to, int num)
        {
            if (num > 0)
            {
                var transferCommand = new TransferCommandReceived()
                {
                    RequestId = Guid.NewGuid(),
                    PartitionId = from,
                    NumActivitiesToSend = num,
                    TransferDestination = to,
                    Timestamp = this.GetUniqueTimestamp(),
                };

                this.PendingOnSource.Add(transferCommand);
                this.PendingOnDestination.Add(transferCommand);
                this.BatchSender.Submit(transferCommand);
            }
        }

        void SendSolicitations()
        {
            DateTime timestamp = DateTime.UtcNow;

            // limit solicitation frequency
            if (timestamp - this.LastSolicitation >= SOLICITATION_INTERVAL)
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

                this.LastSolicitation = timestamp;
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

            // trace backlog estimates
            string estimated = string.Join(",", this.LoadInfo.Values.Select(i => i.ProjectedLoad.ToString()));
            string mobile = string.Join(",", this.LoadInfo.Values.Select(i => i.EstimatedMobile.ToString()));
            this.traceHelper.TraceProgress($"BacklogEstimates pending={this.PendingOnSource.Count},{this.PendingOnDestination.Count} RTT={this.EstimatedTransferRTT:f2} estimated=[{estimated}] mobile=[{mobile}]");
        }

        void ProcessTransferCommandAcksAndExpirations(LoadInformationReceived loadInformationReceived)
        {
            this.PendingOnSource = this.PendingOnSource
                .Where(c => !loadInformationReceived.ConfirmsSource(c) 
                    && DateTime.UtcNow - c.Timestamp <= COMMAND_EXPIRATION)
                .ToList();

            this.PendingOnDestination = this.PendingOnDestination
                .Where(c => !loadInformationReceived.ConfirmsDestination(c)
                    && DateTime.UtcNow - c.Timestamp <= COMMAND_EXPIRATION)
                .ToList();
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

                // process acks and expirations
                this.ProcessTransferCommandAcksAndExpirations(loadInformationReceived);

                // solicit load information if there are commands pending,
                // or if the reporting node is above TQS and can offload
                bool solicitInformationFromAllPartitions = 
                    this.PendingOnDestination.Count > 0 
                    || this.PendingOnSource.Count > 0
                    || (info.ReportedMobile > 0 &&
                        info.AverageActCompletionTime.HasValue &&
                        0.5 * this.TargetQueueSize(info.AverageActCompletionTime.Value) < info.ReportedLoad);
                
                if (solicitInformationFromAllPartitions)
                {
                    this.SendSolicitations();
                }

                this.traceHelper.TraceProgress(
                    $"LoadInformation partition={loadInformationReceived.PartitionId} mobile={info.ReportedMobile} stationary={info.ReportedStationary} ACT={info.AverageActCompletionTime} RTT={transferRTT} AvgRTT={this.EstimatedTransferRTT} solicit={solicitInformationFromAllPartitions} reporting={this.LoadInfo.Count}");

                // if we have load information from all partitions, try to see if we 
                // should transfer load to this node
                if (this.LoadInfo.Count == this.host.NumberPartitions)
                {
                    // compute estimated backlog sizes based on reported size and pending commands
                    this.ComputeBacklogEstimates();

                    if (this.LoadInfo.Values.Select(i => i.EstimatedMobile).Sum() > 0)
                    {
                        // create a default estimation of average activity completion time
                        // (we use this to replace missing estimates)
                        double DefaultEstimatedCompletionTime = this.LoadInfo
                            .Select(t => t.Value.AverageActCompletionTime).Average() ?? TimeSpan.FromMinutes(1).TotalMilliseconds;
                        double EstimatedActCompletionTime(uint partitionId)
                            => this.LoadInfo[partitionId].AverageActCompletionTime ?? DefaultEstimatedCompletionTime;

                        this.DecideLoadTransfers(loadInformationReceived.PartitionId, EstimatedActCompletionTime);
                    }
                }
            }
            catch (Exception e)
            {
                this.traceHelper.TraceError($"Caught exception: ", e);
            }
        }

        double PlanAheadInterval => Math.Min(
                MAX_PLANAHEAD_INTERVAL.TotalMilliseconds,
                (this.EstimatedTransferRTT + EXTRA_BUFFER_RESERVE.TotalMilliseconds));

        int TargetQueueSize(double estimatedActCompletionTime) =>
            (int)Math.Ceiling(this.PlanAheadInterval * this.host.Settings.MaxConcurrentActivityFunctions / estimatedActCompletionTime);

        void DecideLoadTransfers(uint destination, Func<uint, double> EstimatedActCompletionTime)
        {
            var transferTargets = new int[this.host.NumberPartitions];

            // get the current and target queue length
            int reportedQueueSize = this.LoadInfo[destination].ReportedLoad;
            int projectedQueueSize = this.LoadInfo[destination].ProjectedLoad;
            int targetQueueSize = this.TargetQueueSize(EstimatedActCompletionTime(destination));
            int howMuchToPull = Math.Max(0, targetQueueSize - projectedQueueSize);
            int transferCount = 0;

            // If we have not received actual latency measurements from this node, limit transfer size
            if (!this.LoadInfo[destination].AverageActCompletionTime.HasValue)
            {
                howMuchToPull = (int)Math.Ceiling((double)howMuchToPull / this.host.NumberPartitions);
            }

            // while below target, try to find transfer candidates
            for (uint i = 0; i < this.host.NumberPartitions - 1 && howMuchToPull > 0; i++)
            {
                uint candidate = (destination + i + 1) % this.host.NumberPartitions;
                int candidateCurrentQueueSize = this.LoadInfo[candidate].ProjectedLoad;
                int candidateTargetQueueSize = this.TargetQueueSize(EstimatedActCompletionTime(candidate));

                int availableToPull = Math.Min(
                    candidateCurrentQueueSize - candidateTargetQueueSize,
                    this.LoadInfo[candidate].EstimatedMobile);

                int amount = Math.Min(availableToPull, howMuchToPull);


                if (amount > 0)
                {
                    howMuchToPull -= amount;
                    transferTargets[candidate] = amount;
                    transferCount += amount;
                }
            }

            this.traceHelper.TraceProgress($"TransferDecision partition={destination} ACT={EstimatedActCompletionTime(destination)/1000:f2}s PI={this.PlanAheadInterval/1000:F2}s RQS={reportedQueueSize} PQS={projectedQueueSize} TQS={targetQueueSize} Target={howMuchToPull+transferCount} Actual={transferCount}");
        
            if (transferCount > 0)
            {
 
                // send transfer commands
                for (uint i = 0; i < transferTargets.Length; i++)
                {
                    this.SendTransferCommand(i, destination, transferTargets[i]);
                }
            }
        }
    }
}
