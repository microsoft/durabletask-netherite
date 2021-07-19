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

        // data structure for the load monitor (partition id -> backlog size)
        Dictionary<uint, int> LoadInfo { get; set; }

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
            this.LoadInfo = new Dictionary<uint, int>();
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
            this.LoadInfo[loadInformationReceived.PartitionId] = loadInformationReceived.BacklogSize;
            this.traceHelper.TraceWarning($"Received Load information from partition{loadInformationReceived.PartitionId} with load={loadInformationReceived.BacklogSize}");

            // check if the load is greater than the threshold
            // xz: in the future, we may also want to check for overloaded nodes if there are multiple partitions on a single node
            if (loadInformationReceived.BacklogSize > OVERLOAD_THRESHOLD)
            {
                // start the task migration process and find offload targets
                if (this.FindOffloadTarget(loadInformationReceived.PartitionId, loadInformationReceived.BacklogSize - OVERLOAD_THRESHOLD, out Dictionary<uint, int> OffloadTargets))
                {
                    // send offload commands
                    foreach(var kvp in OffloadTargets)
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
                    this.LoadInfo[loadInformationReceived.PartitionId] -= OffloadTargets.Skip(1).Sum(x => x.Value);
                }
            }
        }

        bool FindOffloadTarget(uint currentPartitionId, int numActivitiesToOffload, out Dictionary<uint, int> OffloadTargets)
        {
            OffloadTargets = new Dictionary<uint, int>();
            for (uint i = 0; i < this.host.NumberPartitions - 1 && numActivitiesToOffload > 0; i++)
            {
                uint target = (currentPartitionId + i + 1) % this.host.NumberPartitions;
                if (this.LoadInfo[target] < UNDERLOAD_THRESHOLD)
                {
                    int capacity = OVERLOAD_THRESHOLD - this.LoadInfo[target];
                    OffloadTargets[target] = numActivitiesToOffload - capacity > 0 ? capacity : numActivitiesToOffload;
                    numActivitiesToOffload -= capacity;
                }
            }
            return OffloadTargets.Count == 0 ? false : true;
        }
    }
}
