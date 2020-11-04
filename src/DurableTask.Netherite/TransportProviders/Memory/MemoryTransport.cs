// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Emulated
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// An transport provider that emulates all the communication queues in memory. Meant for testing 
    /// and benchmarking only. It is not distributable,
    /// i.e. can execute only on a single node.
    /// </summary>
    class MemoryTransport : ITaskHub
    {
        readonly TransportAbstraction.IHost host;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly uint numberPartitions;
        readonly ILogger logger;

        Dictionary<Guid, IMemoryQueue<ClientEvent>> clientQueues;
        IMemoryQueue<PartitionEvent>[] partitionQueues;
        TransportAbstraction.IClient client;
        TransportAbstraction.IPartition[] partitions;
        CancellationTokenSource shutdownTokenSource;

        static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public MemoryTransport(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, ILogger logger)
        {
            this.host = host;
            this.settings = settings;
            TransportConnectionString.Parse(settings.EventHubsConnectionString, out _, out _, out int? numberPartitions);
            this.numberPartitions = (uint) numberPartitions.Value;
            this.logger = logger;
        }

        async Task ITaskHub.CreateAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            this.clientQueues = new Dictionary<Guid, IMemoryQueue<ClientEvent>>();
            this.partitionQueues = new IMemoryQueue<PartitionEvent>[this.numberPartitions];
            this.partitions = new TransportAbstraction.IPartition[this.numberPartitions];
        }

        Task ITaskHub.DeleteAsync()
        {
            this.clientQueues = null;
            this.partitionQueues = null;

            return this.host.StorageProvider.DeleteAllPartitionStatesAsync();
        }

        async Task<bool> ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            return this.partitionQueues != null;
        }

        async Task ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            this.host.NumberPartitions = this.numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[this.numberPartitions];

            // create a client
            var clientId = Guid.NewGuid();
            var clientSender = new SendWorker(this.shutdownTokenSource.Token);
            this.client = this.host.AddClient(clientId, default, clientSender);
            var clientQueue = new MemoryClientQueue(this.client, this.shutdownTokenSource.Token, this.logger);
            this.clientQueues[clientId] = clientQueue;
            clientSender.SetHandler(list => this.SendEvents(this.client, list));

            // create all partitions
            var tasks = new List<Task>();
            for (uint i = 0; i < this.numberPartitions; i++)
            {
                tasks.Add(StartPartition(i));
            }
            await Task.WhenAll(tasks);

            async Task StartPartition(uint partitionId)
            {
                var partitionSender = new SendWorker(this.shutdownTokenSource.Token);
                var partition = this.host.AddPartition(partitionId, partitionSender);
                partitionSender.SetHandler(list => this.SendEvents(partition, list));
                this.partitionQueues[partitionId] = new MemoryPartitionQueue(partition, this.shutdownTokenSource.Token, this.logger);
                this.partitions[partitionId] = partition;
                var nextInputQueuePosition = await partition.CreateOrRestoreAsync(this.host.CreateErrorHandler(partitionId), 0).ConfigureAwait(false);
                this.partitionQueues[partitionId].FirstInputQueuePosition = nextInputQueuePosition;
            };

            // start all the emulated queues
            foreach (var partitionQueue in this.partitionQueues)
            {
                partitionQueue.Resume();
            }
            clientQueue.Resume();
        }

        async Task ITaskHub.StopAsync(bool isForced)
        {
            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;

                await this.client.StopAsync().ConfigureAwait(false);

                var tasks = new List<Task>();
                foreach(var p in this.partitions)
                {
                    tasks.Add(p.StopAsync(isForced));
                }
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        void SendEvents(TransportAbstraction.IClient client, IEnumerable<Event> events)
        {
            try
            {
                this.SendEvents(events, null);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                client.ReportTransportError(nameof(SendEvents), e);
            }
        }

        void SendEvents(TransportAbstraction.IPartition partition, IEnumerable<Event> events)
        {
            try
            {
                this.SendEvents(events, partition.PartitionId);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                partition.ErrorHandler.HandleError(nameof(SendEvents), "Encountered exception while trying to send events", e, true, false);
            }
        }

        void SendEvents(IEnumerable<Event> events, uint? sendingPartition)
        {
            foreach (var evt in events)
            {
                if (evt is ClientEvent clientEvent)
                {
                    if (this.clientQueues.TryGetValue(clientEvent.ClientId, out var queue))
                    {
                        queue.Send(clientEvent);
                    }
                }
                else if (evt is PartitionEvent partitionEvent)
                {
                    this.partitionQueues[partitionEvent.PartitionId].Send(partitionEvent);
                }
            }
        }
    }
}
