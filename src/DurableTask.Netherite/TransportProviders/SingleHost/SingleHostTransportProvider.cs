// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.SingleHostTransport
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.NetworkInformation;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Faster;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// An transport provider that executes on a single node only, using in-memory queues to connect the components.
    /// </summary>
    class SingleHostTransportProvider : ITransportProvider
    {
        readonly TransportAbstraction.IHost host;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly IStorageProvider storage;
        readonly uint numberPartitions;
        readonly ILogger logger;
        readonly FaultInjector faultInjector;
        readonly string fingerPrint;

        TaskhubParameters parameters;
        SendWorker[] sendWorkers;
        PartitionQueue[] partitionQueues;
        ClientQueue clientQueue;
        LoadMonitorQueue loadMonitorQueue;

        public SingleHostTransportProvider(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, IStorageProvider storage, ILogger logger)
        {
            this.host = host;
            this.settings = settings;
            this.storage = storage;
            TransportConnectionString.Parse(settings.ResolvedTransportConnectionString, out _, out _);
            this.numberPartitions = (uint) settings.PartitionCount;
            this.logger = logger;
            this.faultInjector = settings.TestHooks?.FaultInjector;
            this.fingerPrint = Guid.NewGuid().ToString("N");
        }
        async Task<TaskhubParameters> ITransportProvider.StartAsync()
        {
            this.parameters = await this.storage.TryLoadTaskhubAsync(throwIfNotFound: true);
            (string containerName, string path) = this.storage.GetTaskhubPathPrefix(this.parameters);
            return this.parameters;
        }

        Task ITransportProvider.StartClientAsync()
        {    
            // create the send workers
            this.sendWorkers = new SendWorker[Environment.ProcessorCount];
            for(int i = 0; i < this.sendWorkers.Length; i++)
            {
                this.sendWorkers[i] = new SendWorker(this, i);
            }
            int nextWorker = 0;
            SendWorker GetAWorker() => this.sendWorkers[(nextWorker++) % this.sendWorkers.Length];
          
            // create the load monitor
            var loadMonitor = this.host.AddLoadMonitor(this.parameters.TaskhubGuid, GetAWorker());
            this.loadMonitorQueue = new LoadMonitorQueue(loadMonitor, this.logger);

            // create the client
            var clientId = Guid.NewGuid();
            var client = this.host.AddClient(clientId, this.parameters.TaskhubGuid, GetAWorker());
            this.clientQueue = new ClientQueue(client, this.logger);

            // create the partition queues
            this.partitionQueues = new PartitionQueue[this.parameters.PartitionCount];
            for (uint i = 0; i < this.partitionQueues.Length; i++)
            {
                this.partitionQueues[i] = new PartitionQueue(this.host, GetAWorker(), i, this.fingerPrint, this.settings.TestHooks, this.parameters, this.logger);
            }

            for (int i = 0; i < this.sendWorkers.Length; i++)
            {
                this.sendWorkers[i].Resume();
            }

            return Task.CompletedTask;
        }

        Task ITransportProvider.StartWorkersAsync()
        {
            // wake up all the workers
            for (uint i = 0; i < this.partitionQueues.Length; i++)
            {
                this.partitionQueues[i].Notify();
            }
            return Task.CompletedTask;
        }

        async Task ITransportProvider.StopAsync()
        {
            var tasks = new List<Task>();
            tasks.Add(this.clientQueue.Client.StopAsync());
            tasks.Add(this.loadMonitorQueue.LoadMonitor.StopAsync());
            foreach (var partitionQueue in this.partitionQueues)
            {
                tasks.Add(partitionQueue.StopAsync());
            }
            await Task.WhenAll(tasks);
        }
        
        class SendWorker : BatchWorker<Event>, TransportAbstraction.ISender
        {
            readonly SingleHostTransportProvider provider;

            public SendWorker(SingleHostTransportProvider provider, int index)
                : base($"SendWorker{index:D2}", true, int.MaxValue, CancellationToken.None, null)
            {
                this.provider = provider;
            }

            protected override Task Process(IList<Event> batch)
            {
                if (batch.Count > 0)
                {
                    try
                    {
                        for(int i = 0; i < batch.Count; i++)
                        {
                            switch(batch[i])
                            {
                                case PartitionEvent partitionEvent:
                                    this.provider.partitionQueues[partitionEvent.PartitionId].Submit(partitionEvent);
                                    break;

                                case ClientEvent clientEvent:
                                    this.provider.clientQueue.Submit(clientEvent);
                                    break;

                                case LoadMonitorEvent loadMonitorEvent:
                                    this.provider.loadMonitorQueue.Submit(loadMonitorEvent);
                                    break;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        System.Diagnostics.Trace.TraceError($"exception in send worker: {e}", e);
                    }
                }

                return Task.CompletedTask;
            }
        }
    }
}
