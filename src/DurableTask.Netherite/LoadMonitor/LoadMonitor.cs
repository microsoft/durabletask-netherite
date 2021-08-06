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
        Dictionary<uint, (int backlogSize, double? AverageActCompletionTime)> LoadInfo { get; set; }

        const string OFFLOAD_METRIC = "Load";
        const int OFFLOAD_MAX_BATCH_SIZE = 20;
        const int OFFLOAD_MIN_BATCH_SIZE = 10;
        const int OVERLOAD_THRESHOLD = 20;
        const int UNDERLOAD_THRESHOLD = 5;

        public static string GetShortId(Guid clientId) => clientId.ToString("N").Substring(0, 7);

        public LoadMonitor(
            NetheriteOrchestrationService host,
            Guid taskHubGuid,
            TransportAbstraction.ISender batchSender)
        {
            this.host = host;
            this.traceHelper = new LoadMonitorTraceHelper(host.Logger, host.Settings.LogLevelLimit, host.StorageAccountName, host.Settings.HubName);
            this.BatchSender = batchSender;
            this.LoadInfo = new Dictionary<uint, (int, double?)>();
            this.traceHelper.TraceWarning("Started");
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

        public void Process(LoadInformationReceived loadInformationReceived)
        {
            this.traceHelper.TraceWarning($"Received Load information from partition{loadInformationReceived.PartitionId} with " +
                $"load={loadInformationReceived.BacklogSize} and avg completion time={loadInformationReceived.AverageActCompletionTime}");

            try
            {
                // update load info
                this.LoadInfo[loadInformationReceived.PartitionId] = (loadInformationReceived.BacklogSize, loadInformationReceived.AverageActCompletionTime);

                // if we don't have information for all partitions yet, do not continue
                if (this.LoadInfo.Count < this.host.NumberPartitions)
                {
                    return;
                }

                // create a default estimation of completion time (we use this to replace missing estimates)
                double DefaultEstimatedCompletionTime = this.LoadInfo.Select(t => t.Value.AverageActCompletionTime).Average() ?? 100;
                double EstimatedActCompletionTime(uint partitionId) => this.LoadInfo[partitionId].AverageActCompletionTime ?? DefaultEstimatedCompletionTime;

                // check if it makes sense to redistribute work from this partition to other partitions
                uint overloadCandidate = loadInformationReceived.PartitionId;
                double AverageWaitTime = this.LoadInfo.Average(t => t.Value.backlogSize * EstimatedActCompletionTime(t.Key));
                double CompletionTime = this.LoadInfo[overloadCandidate].backlogSize * EstimatedActCompletionTime(overloadCandidate);

                // if the expected  completion time is above average, we consider redistributing the load
                if (CompletionTime > AverageWaitTime)
                {
                    // start the task migration process and find offload targets
                    //if (this.FindOffloadTargetByLoad(loadInformationReceived.PartitionId, loadInformationReceived.BacklogSize - OVERLOAD_THRESHOLD, out Dictionary<uint, int> OffloadTargets))
                    if (FindOffloadTargetByWaitTime(this.LoadInfo[overloadCandidate].backlogSize - OVERLOAD_THRESHOLD, out Dictionary<uint, int> OffloadTargets))
                    {
                        // send offload commands
                        foreach (var kvp in OffloadTargets)
                        {
                            this.traceHelper.TraceWarning($"Sending offloadCommand to partition {overloadCandidate} " +
                                $"to send {kvp.Value} activities to partition {kvp.Key}");
                            this.BatchSender.Submit(new OffloadCommandReceived()
                            {
                                RequestId = Guid.NewGuid(),
                                PartitionId = overloadCandidate,
                                NumActivitiesToSend = kvp.Value,
                                OffloadDestination = kvp.Key,
                            });
                        }

                        // load decisions and update load info
                        this.LoadInfo[overloadCandidate] = (this.LoadInfo[overloadCandidate].backlogSize
                            - OffloadTargets.Sum(x => x.Value), this.LoadInfo[overloadCandidate].AverageActCompletionTime);
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

                            double localWaitTime = (this.LoadInfo[overloadCandidate].backlogSize - portionSize) * EstimatedActCompletionTime(overloadCandidate);
                            double remoteWaitTime = targetInfo.backlogSize * EstimatedActCompletionTime(target);

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
            catch (Exception e)
            {
                this.traceHelper.TraceError($"Caught exception: ", e);
            }
        } 

        bool FindOffloadTargetByLoad(uint currentPartitionId, int numActivitiesToOffload, out Dictionary<uint, int> OffloadTargets)
        {
            OffloadTargets = new Dictionary<uint, int>();
            for (uint i = 0; i < this.host.NumberPartitions - 1 && numActivitiesToOffload > 0; i++)
            {
                uint target = (currentPartitionId + i + 1) % this.host.NumberPartitions;
                if (this.LoadInfo[target].backlogSize < UNDERLOAD_THRESHOLD)
                {
                    int capacity = OVERLOAD_THRESHOLD - this.LoadInfo[target].backlogSize;
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
