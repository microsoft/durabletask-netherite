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
        readonly CancellationToken shutdownToken;
        readonly LoadMonitorTraceHelper traceHelper;

        TransportAbstraction.ISender BatchSender { get; set; }

        // data structure for the load monitor (partition id -> (backlog size, average activity completion time))
        Dictionary<uint, (int backlogSize, double AverageActCompletionTime)> LoadInfo { get; set; }

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
            this.LoadInfo = new Dictionary<uint, (int, double)>();
            this.traceHelper.TraceWarning("Started");
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
            // update load info
            this.LoadInfo[loadInformationReceived.PartitionId] = (loadInformationReceived.BacklogSize, loadInformationReceived.AverageActCompletionTime);
            this.traceHelper.TraceWarning($"Received Load information from partition{loadInformationReceived.PartitionId} with " +
                $"load={loadInformationReceived.BacklogSize} and avg completion time={loadInformationReceived.AverageActCompletionTime}");

            double AverageWaitTime = this.LoadInfo.Select(t => t.Value).Average(t => t.backlogSize * t.AverageActCompletionTime);
            double CompletionTime = this.LoadInfo[loadInformationReceived.PartitionId].backlogSize * this.LoadInfo[loadInformationReceived.PartitionId].AverageActCompletionTime;

            //if (loadInformationReceived.BacklogSize > OVERLOAD_THRESHOLD)
            if (CompletionTime > AverageWaitTime)    
            {
                // start the task migration process and find offload targets
                //if (this.FindOffloadTargetByLoad(loadInformationReceived.PartitionId, loadInformationReceived.BacklogSize - OVERLOAD_THRESHOLD, out Dictionary<uint, int> OffloadTargets))
                if (this.FindOffloadTargetByWaitTime(loadInformationReceived.PartitionId, loadInformationReceived.BacklogSize - OVERLOAD_THRESHOLD, 
                    loadInformationReceived.AverageActCompletionTime, AverageWaitTime, out Dictionary<uint, int> OffloadTargets))
                {
                    // send offload commands
                    foreach (var kvp in OffloadTargets)
                    {
                        this.traceHelper.TraceWarning($"Sending offloadCommand to partition {loadInformationReceived.PartitionId} " +
                            $"to send {kvp.Value} activities to partition {kvp.Key}");
                        this.BatchSender.Submit(new OffloadCommandReceived()
                        {
                            RequestId = Guid.NewGuid(),
                            PartitionId = loadInformationReceived.PartitionId,
                            NumActivitiesToSend = kvp.Value,
                            OffloadDestination = kvp.Key,
                        });
                    }

                    // load decisions and update load info
                    this.LoadInfo[loadInformationReceived.PartitionId] = (this.LoadInfo[loadInformationReceived.PartitionId].backlogSize 
                        - OffloadTargets.Sum(x => x.Value), this.LoadInfo[loadInformationReceived.PartitionId].AverageActCompletionTime);
                }
            }


        }

        bool FindOffloadTargetByWaitTime(uint currentPartitionId, int numActivitiesToOffload, double AverageActCompletionTime, double AverageWaitTime, out Dictionary<uint, int> OffloadTargets)
        {
            OffloadTargets = new Dictionary<uint, int>();
            for (uint i = 0; i < this.host.NumberPartitions - 1 && numActivitiesToOffload > 0; i++)
            {
                uint target = (currentPartitionId + i + 1) % this.host.NumberPartitions;
                double localWaitTime = (this.LoadInfo[currentPartitionId].backlogSize - OFFLOAD_MAX_BATCH_SIZE) * this.LoadInfo[currentPartitionId].AverageActCompletionTime;
                double remoteWaitTime = this.LoadInfo[target].backlogSize * this.LoadInfo[target].AverageActCompletionTime;

                if (localWaitTime > remoteWaitTime)
                {
                    OffloadTargets[target] = OFFLOAD_MAX_BATCH_SIZE;
                    numActivitiesToOffload -= OFFLOAD_MAX_BATCH_SIZE;
                }
            }

            return OffloadTargets.Count == 0 ? false : true;
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

        // Do we really need this interval?
        void updateLoadMonitorInterval()
        {
            double utilization = this.LoadInfo.Sum(x => x.Value.backlogSize) / (this.LoadInfo.Count * UNDERLOAD_THRESHOLD);

            if (utilization < 1)
            {
                this.BatchSender.Submit(new ProbingControlReceived()
                {
                    LoadMonitorInterval = TimeSpan.FromMilliseconds(100),
                });
            }
        }
    }
}
