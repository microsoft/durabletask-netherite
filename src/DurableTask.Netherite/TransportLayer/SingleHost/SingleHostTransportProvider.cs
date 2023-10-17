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
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// An transport layer that executes on a single node only, using in-memory queues to connect the components.
    /// </summary>
    class SingleHostTransportLayer : ITransportLayer
    {
        readonly TransportAbstraction.IHost host;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly IStorageLayer storage;
        readonly uint numberPartitions;
        readonly ILogger logger;
        readonly FaultInjector faultInjector;
        readonly string fingerPrint;

        TaskhubParameters parameters;
        SendWorker[] sendWorkers;
        PartitionQueue[] partitionQueues;
        ClientQueue clientQueue;
        LoadMonitorQueue loadMonitorQueue;

        public SingleHostTransportLayer(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, IStorageLayer storage, ILogger logger)
        {
            this.host = host;
            this.settings = settings;
            this.storage = storage;
            this.numberPartitions = (uint) settings.PartitionCount;
            this.logger = logger;
            this.faultInjector = settings.TestHooks?.FaultInjector;
            this.fingerPrint = Guid.NewGuid().ToString("N");
        }
        async Task<TaskhubParameters> ITransportLayer.StartAsync()
        {
            this.parameters = await this.storage.TryLoadTaskhubAsync(throwIfNotFound: true);
            (string containerName, string path) = this.storage.GetTaskhubPathPrefix(this.parameters);
            return this.parameters;
        }

        Task ITransportLayer.StartClientAsync()
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

        Task ITransportLayer.StartWorkersAsync()
        {
            // wake up all the workers
            for (uint i = 0; i < this.partitionQueues.Length; i++)
            {
                this.partitionQueues[i].Notify();
            }
            return Task.CompletedTask;
        }

        async Task ITransportLayer.StopAsync(bool fatalExceptionObserved)
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
            readonly SingleHostTransportLayer transport;

            public SendWorker(SingleHostTransportLayer transport, int index)
                : base($"SendWorker{index:D2}", true, int.MaxValue, CancellationToken.None, null)
            {
                this.transport = transport;
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
                                    this.transport.partitionQueues[partitionEvent.PartitionId].Submit(partitionEvent);
                                    break;

                                case ClientEvent clientEvent:
                                    this.transport.clientQueue.Submit(clientEvent);
                                    break;

                                case LoadMonitorEvent loadMonitorEvent:
                                    this.transport.loadMonitorQueue.Submit(loadMonitorEvent);
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
