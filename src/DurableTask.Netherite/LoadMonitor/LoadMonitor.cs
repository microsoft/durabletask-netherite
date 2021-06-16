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

        public static string GetShortId(Guid clientId) => clientId.ToString("N").Substring(0, 7);

        public LoadMonitor(
            NetheriteOrchestrationService host,
            Guid taskHubGuid,
            TransportAbstraction.ISender batchSender)
        {
            this.host = host;
            this.traceHelper = new LoadMonitorTraceHelper(host.Logger, host.Settings.LogLevelLimit, host.StorageAccountName, host.Settings.HubName);
            this.BatchSender = batchSender;
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

            this.traceHelper.TraceProgress($"Sending CommandReceived to partition {loadInformationReceived.PartitionId}");

            // tells the partition from which we received the load information to offload to the next partition
            this.BatchSender.Submit(new OffloadCommandReceived() 
            { 
                RequestId = Guid.NewGuid(),
                PartitionId = loadInformationReceived.PartitionId,
                OffloadDestination = (loadInformationReceived.PartitionId + 1) % this.host.NumberPartitions,      
            });
        }
    }
}
