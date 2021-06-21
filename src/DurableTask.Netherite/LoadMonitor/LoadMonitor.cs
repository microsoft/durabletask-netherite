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
        const int LOAD_THRESHOLD = int.MaxValue;

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
            this.traceHelper.TraceProgress("Started");
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
            //TODO this is of course not the algorithm we want but it was useful to debug the transport layer

            // update load info
            //this.LoadInfo.Add(loadInformationReceived.PartitionId, loadInformationReceived.BacklogSize);

            // check if the load is greater than the threshold
            // xz: in the future, we may also want to check for overloaded nodes if there are multiple partitions on a single node
            //if (loadInformationReceived.BacklogSize > LOAD_THRESHOLD)
            //{
                // start the task migration process 

                // find partitions to offload tasks

                // send offload commands

                // load decisions and update load info
            //}
            

            this.traceHelper.TraceProgress($"Sending CommandReceived to partition {loadInformationReceived.PartitionId}");

            // tells the partition from which we received the load information to offload to the next partition
            this.BatchSender.Submit(new OffloadCommandReceived() 
            { 
                RequestId = Guid.NewGuid(),
                PartitionId = loadInformationReceived.PartitionId,
                NumActivitiesToSend = 10,
                OffloadDestination = (loadInformationReceived.PartitionId + 1) % this.host.NumberPartitions,      
            });;
        }

        bool FindOffloadTarget(uint currentPartitionId, out uint[] target, out int[] batchsize)
        {
            const int size = 10;
            target = new uint[size];
            batchsize = new int[size];
            return false;
        }
    }
}
