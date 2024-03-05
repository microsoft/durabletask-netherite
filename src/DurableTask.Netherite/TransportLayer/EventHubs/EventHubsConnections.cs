// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

    class EventHubsConnections
    {
        readonly ConnectionInfo connectionInfo;
        readonly string[] clientHubs;
        readonly string partitionHub;
        readonly string loadMonitorHub;
        readonly CancellationToken shutdownToken;
        readonly string consumerGroup;

        EventHubClient partitionClient;
        List<EventHubClient> clientClients;
        EventHubClient loadMonitorClient;

        readonly List<(EventHubClient client, string id)> partitionPartitions = new List<(EventHubClient client, string id)>();
        readonly List<(EventHubClient client, string id)> clientPartitions = new List<(EventHubClient client, string id)>();

        public const int NumClientChannels = 2;

        public DateTime CreationTimestamp { get; private set; }

        public ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>> _partitionSenders = new ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>>();
        public ConcurrentDictionary<Guid, EventHubsClientSender> _clientSenders = new ConcurrentDictionary<Guid, EventHubsClientSender>();
        public ConcurrentDictionary<int, LoadMonitorSender> _loadMonitorSenders = new ConcurrentDictionary<int, LoadMonitorSender>();

        public TransportAbstraction.IHost Host { get; set; }
        public EventHubsTraceHelper TraceHelper { get; set; }

        int GetClientBucket(Guid clientId, int index) => (int)((Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) + index) % (uint)this.clientPartitions.Count);

        public EventHubsConnections(
            ConnectionInfo connectionInfo, 
            string partitionHub,
            string[] clientHubs,
            string loadMonitorHub,
            string consumerGroup,
            CancellationToken shutdownToken)
        {
            this.connectionInfo = connectionInfo;
            this.partitionHub = partitionHub;
            this.clientHubs = clientHubs;
            this.loadMonitorHub = loadMonitorHub;
            this.shutdownToken = shutdownToken;
            this.consumerGroup = consumerGroup;
        }

        const string defaultConsumerGroup = "$Default";

        public string Fingerprint => $"{this.connectionInfo.HostName}{this.partitionHub}/{this.CreationTimestamp:o}";

        public async Task StartAsync(TaskhubParameters parameters)
        {
            await Task.WhenAll(
                this.EnsurePartitionsAsync(parameters.PartitionCount), 
                this.EnsureClientsAsync(), 
                this.EnsureLoadMonitorAsync());

            if (this.consumerGroup != defaultConsumerGroup)
            {
                await this.EnsureConsumerGroupsExistAsync();
            }
        }

        public Task StopAsync()
        {
            return Task.WhenAll(
                this.StopClientClients(),
                this.StopPartitionClients(),
                this.StopLoadMonitorClients()
            );
        }

        async Task StopClientClients()
        {
            await Task.WhenAll(this._clientSenders.Values.Select(sender => sender.WaitForShutdownAsync()).ToList());

            if (this.clientClients != null)
            {
                await Task.WhenAll(this.clientClients.Select(client => client.CloseAsync()).ToList());
            }
        }

        async Task StopPartitionClients()
        {
            await Task.WhenAll(this._partitionSenders.Values.Select(sender => sender.WaitForShutdownAsync()).ToList());

            if (this.partitionClient != null)
            {
                await this.partitionClient.CloseAsync();
            }
        }

        async Task StopLoadMonitorClients()
        {
            await Task.WhenAll(this._loadMonitorSenders.Values.Select(sender => sender.WaitForShutdownAsync()).ToList());

            if (this.loadMonitorHub != null)
            {
                await this.loadMonitorClient.CloseAsync();
            }
        }

        const int EventHubCreationRetries = 5;

        async Task EnsureEventHubExistsAsync(string eventHubName, int partitionCount)
        {
            this.TraceHelper.LogDebug("Creating EventHub {name}", eventHubName);
            bool success = await EventHubsUtil.EnsureEventHubExistsAsync(this.connectionInfo, eventHubName, partitionCount, CancellationToken.None);
            if (success)
            {
                this.TraceHelper.LogInformation("Created EventHub {name}", eventHubName, CancellationToken.None);
            }
            else
            {
                this.TraceHelper.LogDebug("Conflict on EventHub {name}", eventHubName, CancellationToken.None);
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        }

        public Task EnsureConsumerGroupsExistAsync()
        {
            return Task.WhenAll(
                EnsureExistsAsync(this.partitionHub),
                EnsureExistsAsync(this.loadMonitorHub),
                Task.WhenAll(this.clientHubs.Select(clientHub => EnsureExistsAsync(clientHub)).ToList())
            );

            async Task EnsureExistsAsync(string eventHubName)
            {
                this.TraceHelper.LogDebug("Creating ConsumerGroup {eventHubName}|{name}", eventHubName, this.consumerGroup);
                bool success = await EventHubsUtil.EnsureConsumerGroupExistsAsync(this.connectionInfo, eventHubName, this.consumerGroup, CancellationToken.None);
                if (success)
                {
                    this.TraceHelper.LogInformation("Created ConsumerGroup {eventHubName}|{name}", eventHubName, this.consumerGroup, CancellationToken.None);
                }
                else
                {
                    this.TraceHelper.LogDebug("Conflict on ConsumerGroup {eventHubName}|{name}", eventHubName, this.consumerGroup, CancellationToken.None);
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }
        }


        internal async Task DeletePartitions()
        {
            await EventHubsUtil.DeleteEventHubIfExistsAsync(this.connectionInfo, this.partitionHub, CancellationToken.None);
        }

        public async Task EnsurePartitionsAsync(int partitionCount, int retries = EventHubCreationRetries)
        {
            var client = this.connectionInfo.CreateEventHubClient(this.partitionHub);
            try
            {
                var runtimeInformation = await client.GetRuntimeInformationAsync();

                if (runtimeInformation.PartitionCount == partitionCount)
                {
                    // we were successful. Record information and create a flat list of partition partitions
                    this.partitionClient = client;
                    this.CreationTimestamp = runtimeInformation.CreatedAt;
                    for (int i = 0; i < partitionCount; i++)
                    {
                        this.partitionPartitions.Add((client, i.ToString()));
                    }
                    return;
                }
                else
                {
                    // we have to create a fresh one
                    this.TraceHelper.LogWarning("Deleting existing partition EventHub because of partition count mismatch.");
                    await EventHubsUtil.DeleteEventHubIfExistsAsync(this.connectionInfo, this.partitionHub, CancellationToken.None);
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException) when (retries > 0)
            {
                await this.EnsureEventHubExistsAsync(this.partitionHub, partitionCount);
            }

            // try again
            if (retries > 0)
            {
                await this.EnsurePartitionsAsync(partitionCount, retries - 1);
            }
            else // should never happen
            {
                this.TraceHelper.LogError("Could not ensure existence of partitions event hub.");
                throw new InvalidOperationException("could not ensure existence of partitions event hub"); 
            }
        }

        async Task EnsureClientsAsync()
        {
            var clientTasks = new List<Task<(EventHubClient, EventHubRuntimeInformation)>>();
            for (int i = 0; i < this.clientHubs.Count(); i++)
            {
                clientTasks.Add(this.EnsureClientAsync(i));
            }
            await Task.WhenAll(clientTasks);

            this.clientClients = clientTasks.Select(t => t.Result.Item1).ToList();
            var clientInfos = clientTasks.Select(t => t.Result.Item2).ToList();

            // create a flat list of client partitions
            for (int i = 0; i < clientTasks.Count(); i++)
            {
                foreach (var id in clientTasks[i].Result.Item2.PartitionIds)
                {
                    this.clientPartitions.Add((this.clientClients[i], id));
                }
            }
        }

        async Task<(EventHubClient, EventHubRuntimeInformation)> EnsureClientAsync(int i, int retries = EventHubCreationRetries)
        {
            try
            {
                var client = this.connectionInfo.CreateEventHubClient(this.clientHubs[i]);
                var runtimeInformation = await client.GetRuntimeInformationAsync();
                return (client, runtimeInformation);
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException) when (retries > 0)
            {
                await this.EnsureEventHubExistsAsync(this.clientHubs[i], 32);
            }
            // try again
            return await this.EnsureClientAsync(i, retries - 1);
        }


        async Task EnsureLoadMonitorAsync(int retries = EventHubCreationRetries)
        {
            // create loadmonitor client
            try
            {
                this.loadMonitorClient = this.connectionInfo.CreateEventHubClient(this.loadMonitorHub);
                var runtimeInformation = await this.loadMonitorClient.GetRuntimeInformationAsync();
                return;
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException) when (retries > 0)
            {
                await this.EnsureEventHubExistsAsync(this.loadMonitorHub, 1);
            }
            // try again
            await this.EnsureLoadMonitorAsync(retries - 1);
        }

        public async Task<List<long>> GetStartingSequenceNumbers()
        {
            Task<long>[] tasks = new Task<long>[this.partitionPartitions.Count];
            for (int i = 0; i < this.partitionPartitions.Count; i++)
            {
                tasks[i] = GetLastEnqueuedSequenceNumber(i);
            }
            await Task.WhenAll(tasks);
            return tasks.Select(t => t.Result).ToList();    

            async Task<long> GetLastEnqueuedSequenceNumber(int i)
            {
                var info = await this.partitionPartitions[i].client.GetPartitionRuntimeInformationAsync(this.partitionPartitions[i].id);
                return info.LastEnqueuedSequenceNumber + 1;
            }
        }

        public static async Task<List<long>> GetQueuePositionsAsync(ConnectionInfo connectionInfo, string partitionHub)
        {
            var client = connectionInfo.CreateEventHubClient(partitionHub);
            try
            {
                var runtimeInformation = await client.GetRuntimeInformationAsync();
                var infoTasks = runtimeInformation.PartitionIds.Select(id => client.GetPartitionRuntimeInformationAsync(id)).ToList();
                await Task.WhenAll(infoTasks);
                return infoTasks.Select(t => t.Result.LastEnqueuedSequenceNumber + 1).ToList();
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException)
            {
                return null;
            }
        }

        // This is to be used when EventProcessorHost is not used.
        public PartitionReceiver CreatePartitionReceiver(int partitionId, string consumerGroupName, long nextPacketToReceive)
        {
            (EventHubClient client, string id) = this.partitionPartitions[partitionId];
            // To create a receiver we need to give it the last! packet number and not the next to receive 
            var eventPosition = EventPosition.FromSequenceNumber(nextPacketToReceive - 1);
            var partitionReceiver = client.CreateReceiver(consumerGroupName, id, eventPosition);
            this.TraceHelper.LogDebug("Created Partition {partitionId} PartitionReceiver {receiver} from {clientId} to read at {position}", partitionId, partitionReceiver.ClientId, client.ClientId, nextPacketToReceive);
            return partitionReceiver;
        }

        public PartitionReceiver[] CreateClientReceivers(Guid clientId, string consumerGroupName)
        {
            var partitionReceivers = new PartitionReceiver[EventHubsConnections.NumClientChannels];
            for (int index = 0; index < EventHubsConnections.NumClientChannels; index++)
            {
                int clientBucket = this.GetClientBucket(clientId, index);
                (EventHubClient client, string id) = this.clientPartitions[clientBucket];
                var clientReceiver = client.CreateReceiver(consumerGroupName, id, EventPosition.FromEnd());
                this.TraceHelper.LogDebug("Created Client {clientId} PartitionReceiver {receiver} from {clientId}", clientId, clientReceiver.ClientId, client.ClientId);
                partitionReceivers[index] = clientReceiver;
            }
            return partitionReceivers;
        }

        public PartitionReceiver CreateLoadMonitorReceiver(string consumerGroupName)
        {
            var loadMonitorReceiver = this.loadMonitorClient.CreateReceiver(consumerGroupName, "0", EventPosition.FromEnqueuedTime(DateTime.UtcNow - TimeSpan.FromSeconds(10)));
            this.TraceHelper.LogDebug("Created LoadMonitor PartitionReceiver {receiver} from {clientId}", loadMonitorReceiver.ClientId, this.loadMonitorClient.ClientId);
            return loadMonitorReceiver;
        }


        public EventHubsSender<PartitionUpdateEvent> GetPartitionSender(int partitionId, byte[] taskHubGuid, NetheriteOrchestrationServiceSettings settings)
        {
            return this._partitionSenders.GetOrAdd(partitionId, (key) => {
                (EventHubClient client, string id) = this.partitionPartitions[partitionId];
                var partitionSender = client.CreatePartitionSender(id);
                var sender = new EventHubsSender<PartitionUpdateEvent>(
                    this.Host,
                    taskHubGuid,
                    partitionSender,
                    this.shutdownToken,
                    this.TraceHelper,
                    settings);
                this.TraceHelper.LogDebug("Created PartitionSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                return sender;
            });
        }

        public EventHubsClientSender GetClientSender(Guid clientId, NetheriteOrchestrationServiceSettings settings)
        {
            return this._clientSenders.GetOrAdd(clientId, (key) =>
            {
                var partitionSenders = new PartitionSender[NumClientChannels];
                for (int index = 0; index < NumClientChannels; index++)
                {
                    int clientBucket = this.GetClientBucket(clientId, index);
                    (EventHubClient client, string id) = this.clientPartitions[clientBucket];
                    partitionSenders[index] = client.CreatePartitionSender(id);
                    this.TraceHelper.LogDebug("Created ClientSender {sender} from {clientId}", partitionSenders[index].ClientId, client.ClientId);
                }
                var sender = new EventHubsClientSender(
                        this.Host,
                        clientId,
                        partitionSenders,
                        this.shutdownToken,
                        this.TraceHelper,
                        settings);
                return sender;
            });
        }

        public LoadMonitorSender GetLoadMonitorSender(byte[] taskHubGuid)
        {
            return this._loadMonitorSenders.GetOrAdd(0, (key) =>
            {
                var loadMonitorSender = this.loadMonitorClient.CreatePartitionSender("0");
                var sender = new LoadMonitorSender(
                    this.Host,
                    taskHubGuid,
                    loadMonitorSender,
                    this.shutdownToken,
                    this.TraceHelper);
                this.TraceHelper.LogDebug("Created LoadMonitorSender {sender} from {clientId}", loadMonitorSender.ClientId, this.loadMonitorClient.ClientId);
                return sender;
            });
        }
    }
}
