// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Emulated
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Faster;
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
        readonly FaultInjector faultInjector;

        int epoch;
        Task startOrRecoverTask;

        Dictionary<Guid, IMemoryQueue<ClientEvent>> clientQueues;
        IMemoryQueue<PartitionEvent>[] partitionQueues;
        IMemoryQueue<LoadMonitorEvent> loadMonitorQueue;
        TransportAbstraction.IClient client;
        TransportAbstraction.IPartition[] partitions;
        TransportAbstraction.ILoadMonitor loadMonitor;
        IPartitionErrorHandler[] partitionErrorHandlers;
        CancellationTokenSource shutdownTokenSource;

        static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public MemoryTransport(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, ILogger logger)
        {
            this.host = host;
            this.settings = settings;
            TransportConnectionString.Parse(settings.ResolvedTransportConnectionString, out _, out _);
            this.numberPartitions = (uint) settings.PartitionCount;
            this.logger = logger;
            this.faultInjector = settings.FaultInjector;
        }

        async Task<bool> ITaskHub.CreateIfNotExistsAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            this.clientQueues = new Dictionary<Guid, IMemoryQueue<ClientEvent>>();
            this.partitionQueues = new IMemoryQueue<PartitionEvent>[this.numberPartitions];
            this.partitions = new TransportAbstraction.IPartition[this.numberPartitions];
            this.partitionErrorHandlers = new IPartitionErrorHandler[this.numberPartitions];
            return true;
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

        Task ITaskHub.StartAsync()
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

            // create a load monitor
            if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
            {
                var loadMonitorSender = new SendWorker(this.shutdownTokenSource.Token);
                this.loadMonitor = this.host.AddLoadMonitor(default, loadMonitorSender);
                this.loadMonitorQueue = new MemoryLoadMonitorQueue(this.loadMonitor, this.shutdownTokenSource.Token, this.logger);
                loadMonitorSender.SetHandler(list => this.SendEvents(this.loadMonitor, list));
            }

            // we finish the (possibly lengthy) partition loading asynchronously so it is possible to receive 
            // stop signals before partitions are fully recovered
            this.startOrRecoverTask = this.StartOrRecoverAsync(0);

            return Task.CompletedTask;
        }

        void RecoveryHandler(int epoch)
        {
            if (this.epoch != epoch
                || Interlocked.CompareExchange(ref this.epoch, epoch + 1, epoch) != epoch)
            {
                return;
            }

            if (this.shutdownTokenSource?.IsCancellationRequested == false)
            {
                this.startOrRecoverTask = this.StartOrRecoverAsync(epoch + 1);
            }
        }

        async Task StartOrRecoverAsync(int epoch)
        {
            await Task.Yield();
            System.Diagnostics.Debug.Assert(this.startOrRecoverTask != null);

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

                this.partitionQueues[partitionId] = this.faultInjector == null ?
                    new MemoryPartitionQueueWithSerialization(partition, this.shutdownTokenSource.Token, this.logger)
                    : new MemoryPartitionQueue(partition, this.shutdownTokenSource.Token, this.logger); // need durability listeners to be correctly notified
                
                this.partitions[partitionId] = partition;
                this.partitionErrorHandlers[partitionId] = this.host.CreateErrorHandler(partitionId);

                if (this.faultInjector != null)
                {
                    this.partitionErrorHandlers[partitionId].Token.Register(() => this.RecoveryHandler(epoch));
                }

                var nextInputQueuePosition = await partition.CreateOrRestoreAsync(this.partitionErrorHandlers[partitionId], 0);
                this.partitionQueues[partitionId].FirstInputQueuePosition = nextInputQueuePosition;
            };

            this.shutdownTokenSource?.Token.ThrowIfCancellationRequested();

            System.Diagnostics.Trace.TraceInformation($"MemoryTransport: Recovered epoch={epoch}");

            // start all the emulated queues
            foreach (var partitionQueue in this.partitionQueues)
            {
                partitionQueue.Resume();
            }
            foreach (var clientQueue in this.clientQueues.Values)
            {
                clientQueue.Resume();
            }
            this.loadMonitorQueue.Resume();
        }

        async Task ITaskHub.StopAsync()
        {
            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;

                try
                {
                    await (this.startOrRecoverTask ?? Task.CompletedTask);
                }
                catch(OperationCanceledException)
                {
                    // normal if shut down during startup
                }

                await this.client.StopAsync().ConfigureAwait(false);

                var tasks = new List<Task>();
                foreach(var p in this.partitions)
                {
                    tasks.Add(p.StopAsync(false));
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

        void SendEvents(TransportAbstraction.ILoadMonitor loadMonitor, IEnumerable<Event> events)
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
                System.Diagnostics.Trace.TraceError($"MemoryTransport: send exception {e}");
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
                else if (evt is LoadMonitorEvent loadMonitorEvent)
                {
                    this.loadMonitorQueue.Send(loadMonitorEvent);
                }
            }
        }
    }
}
