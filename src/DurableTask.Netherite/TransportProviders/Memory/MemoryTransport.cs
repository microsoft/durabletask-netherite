// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Emulated
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
        IMemoryQueue<LoadMonitorEvent> loadMonitorQueue;
        TransportAbstraction.IClient client;
        TransportAbstraction.ILoadMonitor loadMonitor;
        CancellationTokenSource shutdownTokenSource;

        SendWorker clientSender;
        SendWorker loadMonitorSender;

        IMemoryQueue<PartitionEvent>[] partitionQueues;
        TransportAbstraction.IPartition[] partitions;

        static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public MemoryTransport(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, ILogger logger)
        {
            this.host = host;
            this.settings = settings;
            TransportConnectionString.Parse(settings.ResolvedTransportConnectionString, out _, out _);
            this.numberPartitions = (uint) settings.PartitionCount;
            this.logger = logger;
            this.faultInjector = settings.TestHooks?.FaultInjector;
        }

        async Task<bool> ITaskHub.CreateIfNotExistsAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            this.clientQueues = new Dictionary<Guid, IMemoryQueue<ClientEvent>>();
            return true;
        }

        Task ITaskHub.DeleteAsync()
        {
            this.clientQueues = null;
            return this.host.StorageProvider.DeleteTaskhubAsync("");
        }

        async Task<bool> ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            return this.clientQueues != null;
        }

        Task ITaskHub.StartClientAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            this.host.NumberPartitions = this.numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[this.numberPartitions];

            // create a client
            var clientId = Guid.NewGuid();
            this.clientSender = new SendWorker(this.shutdownTokenSource.Token);
            this.client = this.host.AddClient(clientId, default, this.clientSender);
            var clientQueue = new MemoryClientQueue(this.client, this.shutdownTokenSource.Token, this.logger);
            this.clientQueues[clientId] = clientQueue;
            this.clientSender.SetHandler(list => this.SendEvents(this.client, list));

            return Task.CompletedTask;
        }

        Task ITaskHub.StartWorkersAsync()
        {
            // create a load monitor
            if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
            {
                this.loadMonitorSender = new SendWorker(this.shutdownTokenSource.Token);
                this.loadMonitor = this.host.AddLoadMonitor(default, this.loadMonitorSender);
                this.loadMonitorQueue = new MemoryLoadMonitorQueue(this.loadMonitor, this.shutdownTokenSource.Token, this.logger);
                this.loadMonitorSender.SetHandler(list => this.SendEvents(this.loadMonitor, list));
                this.loadMonitorSender.Resume();
            }

            // we finish the (possibly lengthy) partition loading asynchronously so it is possible to receive 
            // stop signals before partitions are fully recovered
            var greenLight = new TaskCompletionSource<bool>();
            this.startOrRecoverTask = this.StartOrRecoverAsync(0, greenLight.Task);
            greenLight.SetResult(true);

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
                var greenLight = new TaskCompletionSource<bool>();
                this.startOrRecoverTask = this.StartOrRecoverAsync(epoch + 1, greenLight.Task);
                greenLight.SetResult(true);
            }
        }

        async Task StartOrRecoverAsync(int epoch, Task<bool> greenLight)
        {
            if (!await greenLight) return;

            if (epoch > 0)
            {
                if (this.settings.TestHooks != null && this.settings.TestHooks.FaultInjector == null)
                {
                    this.settings.TestHooks.Error("MemoryTransport", "Unexpected partition termination");
                }

                // stop all partitions that are not already terminated
                foreach (var partition in this.partitions)
                {
                    if (!partition.ErrorHandler.IsTerminated)
                    {
                        partition.ErrorHandler.HandleError("MemoryTransport.StartOrRecoverAsync", "recovering all partitions", null, true, true);
                    }
                }
            }

            var partitions = this.partitions = new TransportAbstraction.IPartition[this.numberPartitions];
            var partitionQueues = this.partitionQueues = new IMemoryQueue<PartitionEvent>[this.numberPartitions];
            var partitionSenders = new SendWorker[this.numberPartitions];

            if (epoch == 0)
            {
                this.loadMonitorSender.Resume();
                this.loadMonitorQueue.Resume();
                this.clientSender.Resume();
                foreach (var clientQueue in this.clientQueues.Values)
                {
                    clientQueue.Resume();
                }
            }

            // create the partitions, partition senders, and partition queues
            for (int i = 0; i < this.numberPartitions; i++)
            {
                var partitionSender = partitionSenders[i] = new SendWorker(this.shutdownTokenSource.Token);
                var partition = this.host.AddPartition((uint) i, partitionSender);
                partitionSender.SetHandler(list => this.SendEvents(partition, list, partitionQueues));

                partitionQueues[i] = this.faultInjector == null ?
                    new MemoryPartitionQueueWithSerialization(partition, this.shutdownTokenSource.Token, this.logger)
                    : new MemoryPartitionQueue(partition, this.shutdownTokenSource.Token, this.logger); // need durability listeners to be correctly notified

                partitions[i] = partition;
            }

            // start all the partitions
            var tasks = new List<Task>();
            for (int i = 0; i < this.numberPartitions; i++)
            {
                tasks.Add(StartPartition(i));
            }
            await Task.WhenAll(tasks);

            async Task StartPartition(int i)
            {
                partitionSenders[i].Resume();

                var errorHandler = this.host.CreateErrorHandler((uint)i);
                if (this.faultInjector != null)
                {
                    errorHandler.Token.Register(() => this.RecoveryHandler(epoch));
                }
                var nextInputQueuePosition = await partitions[i].CreateOrRestoreAsync(errorHandler, 0);

                // start delivering events to the partition
                partitionQueues[i].FirstInputQueuePosition = nextInputQueuePosition;
                partitionQueues[i].Resume();
            };

            this.shutdownTokenSource?.Token.ThrowIfCancellationRequested();

            System.Diagnostics.Trace.TraceInformation($"MemoryTransport: Recovered epoch={epoch}");
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
                this.SendEvents(events, null, this.partitionQueues);
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

        void SendEvents(TransportAbstraction.IPartition partition, IEnumerable<Event> events, IMemoryQueue<PartitionEvent>[] partitionQueues)
        {
            try
            {
                this.SendEvents(events, partition.PartitionId, partitionQueues);
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
                this.SendEvents(events, null, this.partitionQueues);
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

        void SendEvents(IEnumerable<Event> events, uint? sendingPartition, IMemoryQueue<PartitionEvent>[] partitionQueues)
        {
            foreach (var evt in events)
            {
                if (evt is ClientEvent clientEvent)
                {
                    if (this.clientQueues.TryGetValue(clientEvent.ClientId, out var queue))
                    {
                        queue.Send(clientEvent);
                    }
                    else
                    {
                        // client does not exist, can happen after recovery
                        DurabilityListeners.ConfirmDurable(clientEvent);
                    }
                }
                else if (evt is PartitionEvent partitionEvent)
                {
                    partitionQueues[partitionEvent.PartitionId].Send(partitionEvent);
                }
                else if (evt is LoadMonitorEvent loadMonitorEvent)
                {
                    this.loadMonitorQueue.Send(loadMonitorEvent);
                }
            }
        }
    }
}
