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
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Messaging.EventHubs.Producer;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Extensions.Azure;
    using Microsoft.Extensions.Logging;

    class EventHubsConnections
    {
        readonly ConnectionInfo connectionInfo;
        readonly string[] clientHubs;
        readonly string partitionHub;
        readonly string loadMonitorHub;
        readonly CancellationToken shutdownToken;

        EventHubConnection partitionConnection;
        List<EventHubConnection> clientConnection;
        EventHubConnection loadMonitorConnection;

        readonly List<(EventHubConnection connection, string id)> partitionPartitions = new List<(EventHubConnection connection, string id)>();
        readonly List<(EventHubConnection connection, string id)> clientPartitions = new List<(EventHubConnection connection, string id)>();

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
            CancellationToken shutdownToken)
        {
            this.connectionInfo = connectionInfo;
            this.partitionHub = partitionHub;
            this.clientHubs = clientHubs;
            this.loadMonitorHub = loadMonitorHub;
            this.shutdownToken = shutdownToken;
        }

        public string Fingerprint => $"{this.connectionInfo.HostName}{this.partitionHub}/{this.CreationTimestamp:o}";

        public async Task StartAsync(TaskhubParameters parameters)
        {
            await Task.WhenAll(
                this.EnsurePartitionsAsync(parameters.PartitionCount), 
                this.EnsureClientsAsync(), 
                this.EnsureLoadMonitorAsync());
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

            if (this.clientConnection != null)
            {
                await Task.WhenAll(this.clientConnection.Select(client => client.CloseAsync()).ToList());
            }
        }

        async Task StopPartitionClients()
        {
            await Task.WhenAll(this._partitionSenders.Values.Select(sender => sender.WaitForShutdownAsync()).ToList());

            if (this.partitionConnection != null)
            {
                await this.partitionConnection.CloseAsync();
            }
        }

        async Task StopLoadMonitorClients()
        {
            await Task.WhenAll(this._loadMonitorSenders.Values.Select(sender => sender.WaitForShutdownAsync()).ToList());

            if (this.loadMonitorHub != null)
            {
                await this.loadMonitorConnection.CloseAsync();
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

        internal async Task DeletePartitions()
        {
            await EventHubsUtil.DeleteEventHubIfExistsAsync(this.connectionInfo, this.partitionHub, CancellationToken.None);
        }

        public async Task EnsurePartitionsAsync(int partitionCount, int retries = EventHubCreationRetries)
        {     
            try
            {
                var connection = this.connectionInfo.CreateEventHubConnection(this.partitionHub);

                await using var client = new EventHubProducerClient(connection);
             
                EventHubProperties eventHubProperties = await client.GetEventHubPropertiesAsync();

                if (eventHubProperties.PartitionIds.Length == partitionCount)
                {
                    // we were successful. Record information and create a flat list of partition partitions
                    this.partitionConnection = connection;
                    this.CreationTimestamp = eventHubProperties.CreatedOn.UtcDateTime;
                    for (int i = 0; i < partitionCount; i++)
                    {
                        this.partitionPartitions.Add((connection, i.ToString()));
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
            catch (Azure.Messaging.EventHubs.EventHubsException e) when (retries > 0 && e.Reason == EventHubsException.FailureReason.ResourceNotFound)
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
            var clientTasks = new List<Task<(EventHubConnection, EventHubProperties)>>();
            for (int i = 0; i < this.clientHubs.Count(); i++)
            {
                clientTasks.Add(this.EnsureClientAsync(i));
            }
            await Task.WhenAll(clientTasks);

            this.clientConnection = clientTasks.Select(t => t.Result.Item1).ToList();
            var clientInfos = clientTasks.Select(t => t.Result.Item2).ToList();

            // create a flat list of client partitions
            for (int i = 0; i < clientTasks.Count(); i++)
            {
                foreach (var id in clientTasks[i].Result.Item2.PartitionIds)
                {
                    this.clientPartitions.Add((this.clientConnection[i], id));
                }
            }
        }

        async Task<(EventHubConnection, EventHubProperties)> EnsureClientAsync(int i, int retries = EventHubCreationRetries)
        {
            try
            {
                var connection = this.connectionInfo.CreateEventHubConnection(this.clientHubs[i]);
                await using var client = new EventHubProducerClient(connection);
                var runtimeInformation = await client.GetEventHubPropertiesAsync();
                return (connection, runtimeInformation);
            }
            catch (Azure.Messaging.EventHubs.EventHubsException e) when (retries > 0 && e.Reason == EventHubsException.FailureReason.ResourceNotFound)
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
                var connection = this.connectionInfo.CreateEventHubConnection(this.loadMonitorHub);
                await using var client = new EventHubProducerClient(connection);
                var runtimeInformation = await client.GetEventHubPropertiesAsync();
                this.loadMonitorConnection = connection;
                return;
            }
            catch (Azure.Messaging.EventHubs.EventHubsException e) when (retries > 0 && e.Reason == EventHubsException.FailureReason.ResourceNotFound)
            {
                await this.EnsureEventHubExistsAsync(this.loadMonitorHub, 1);
            }
            // try again
            await this.EnsureLoadMonitorAsync(retries - 1);
        }

        public static async Task<List<long>> GetQueuePositionsAsync(ConnectionInfo connectionInfo, string partitionHub)
        {
            try
            {
                var connection = connectionInfo.CreateEventHubConnection(partitionHub);
                var client = new EventHubProducerClient(connection);
                var partitions = await client.GetPartitionIdsAsync();
                var infoTasks = partitions.Select(id => client.GetPartitionPropertiesAsync(id)).ToList();
                await Task.WhenAll(infoTasks);
                return infoTasks.Select(t => t.Result.LastEnqueuedSequenceNumber + 1).ToList();
            }
            catch (Azure.Messaging.EventHubs.EventHubsException e) when (e.Reason == EventHubsException.FailureReason.ResourceNotFound)
            {
                return null;
            }
        }

        // This is to be used when EventProcessorHost is not used.
        public PartitionReceiver CreatePartitionReceiver(int partitionId, string consumerGroupName, long nextPacketToReceive)
        {
            (EventHubConnection connection, string id) = this.partitionPartitions[partitionId];
         
            var partitionReceiver = new PartitionReceiver(
                consumerGroupName,
                partitionId.ToString(),
                EventPosition.FromSequenceNumber(nextPacketToReceive - 1),
                connection);      
             
            this.TraceHelper.LogDebug("Created Partition {partitionId} PartitionReceiver {receiver} to read at {position}", partitionId, partitionReceiver.Identifier, nextPacketToReceive);
            return partitionReceiver;
        }

        public PartitionReceiver[] CreateClientReceivers(Guid clientId, string consumerGroupName)
        {
            var partitionReceivers = new PartitionReceiver[EventHubsConnections.NumClientChannels];
            for (int index = 0; index < EventHubsConnections.NumClientChannels; index++)
            {
                int clientBucket = this.GetClientBucket(clientId, index);
                (EventHubConnection connection, string partitionId) = this.clientPartitions[clientBucket];

                var clientReceiver = new PartitionReceiver(
                    consumerGroupName,
                    partitionId.ToString(),
                    EventPosition.Latest,
                    connection); 
                
                this.TraceHelper.LogDebug("Created Client {clientId} PartitionReceiver {receiver}", clientId, clientReceiver.Identifier);
                partitionReceivers[index] = clientReceiver;
            }
            return partitionReceivers;
        }

        public EventHubsSender<PartitionUpdateEvent> GetPartitionSender(int partitionId, byte[] taskHubGuid, NetheriteOrchestrationServiceSettings settings)
        {
            return this._partitionSenders.GetOrAdd(partitionId, (key) => {
                (EventHubConnection connection, string id) = this.partitionPartitions[partitionId];
                var sender = new EventHubsSender<PartitionUpdateEvent>(
                    this.Host,
                    taskHubGuid,
                    connection,
                    id,
                    this.shutdownToken,
                    this.TraceHelper,
                    settings);
                return sender;
            });
        }

        public EventHubsClientSender GetClientSender(Guid clientId, NetheriteOrchestrationServiceSettings settings)
        {
            return this._clientSenders.GetOrAdd(clientId, (key) =>
            {
                var partitions = new (EventHubConnection connection, string partitionId)[NumClientChannels];
                for (int index = 0; index < NumClientChannels; index++)
                {
                    int clientBucket = this.GetClientBucket(clientId, index);
                    partitions[index] = this.clientPartitions[clientBucket];
                }
                var sender = new EventHubsClientSender(
                        this.Host,
                        clientId,
                        partitions,
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
                var sender = new LoadMonitorSender(
                    this.Host,
                    taskHubGuid,
                    this.loadMonitorConnection,
                    this.shutdownToken,
                    this.TraceHelper);
                return sender;
            });
        }
    }
}
