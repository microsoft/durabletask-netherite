// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

    class EventHubsConnections
    {
        readonly string connectionString;
        readonly string[] clientHubs;
        readonly string partitionHub;
        readonly string loadMonitorHub;

        EventHubClient partitionClient;
        List<EventHubClient> clientClients;
        EventHubClient loadMonitorClient;

        readonly List<(EventHubClient client, string id)> partitionPartitions = new List<(EventHubClient client, string id)>();
        readonly List<(EventHubClient client, string id)> clientPartitions = new List<(EventHubClient client, string id)>();

        public const int NumClientChannels = 2;

        public string Endpoint { get; private set; }
        public DateTime CreationTimestamp { get; private set; }

        public ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>> _partitionSenders = new ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>>();
        public ConcurrentDictionary<Guid, EventHubsClientSender> _clientSenders = new ConcurrentDictionary<Guid, EventHubsClientSender>();
        public ConcurrentDictionary<int, LoadMonitorSender> _loadMonitorSenders = new ConcurrentDictionary<int, LoadMonitorSender>();

        public TransportAbstraction.IHost Host { get; set; }
        public EventHubsTraceHelper TraceHelper { get; set; }

        int GetClientBucket(Guid clientId, int index) => (int)((Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) + index) % (uint)this.clientPartitions.Count);

        public EventHubsConnections(
            string connectionString, 
            string partitionHub,
            string[] clientHubs,
            string loadMonitorHub)
        {
            this.connectionString = connectionString;
            this.partitionHub = partitionHub;
            this.clientHubs = clientHubs;
            this.loadMonitorHub = loadMonitorHub;
        }

        public string Fingerprint => $"{this.Endpoint}{this.partitionHub}/{this.CreationTimestamp:o}";

        public async Task StartAsync(TaskhubParameters parameters)
        {
            await Task.WhenAll(
                this.EnsurePartitionsAsync(parameters.PartitionCount), 
                this.EnsureClientsAsync(), 
                this.EnsureLoadMonitorAsync());
        }

        public async Task StopAsync()
        {
            IEnumerable<EventHubClient> Clients()
            {
                if (this.partitionClient != null)
                {
                    yield return this.partitionClient;
                }

                if (this.clientClients != null)
                {
                    foreach (var client in this.clientClients)
                    {
                        yield return client;
                    }
                }

                if (this.loadMonitorHub != null)
                {
                    yield return this.loadMonitorClient;
                }
            }

            await Task.WhenAll(Clients().Select(client => client.CloseAsync()).ToList());
        }

        public async Task EnsurePartitionsAsync(int partitionCount, int retries = 3)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
            {
                EntityPath = this.partitionHub
            };
            var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            try
            {
                var runtimeInformation = await client.GetRuntimeInformationAsync();

                if (runtimeInformation.PartitionCount == partitionCount)
                {
                    // we were successful. Record information and create a flat list of partition partitions
                    this.partitionClient = client;
                    this.Endpoint = connectionStringBuilder.Endpoint.ToString();
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
                    await EventHubsUtil.DeleteEventHubIfExistsAsync(this.connectionString, this.partitionHub);
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException) when (retries > 0)
            {
                this.TraceHelper.LogInformation("Creating EventHub {name}", this.partitionHub);
                await EventHubsUtil.EnsureEventHubExistsAsync(this.connectionString, this.partitionHub, partitionCount);
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

        internal async Task DeletePartitions()
        {
            await EventHubsUtil.DeleteEventHubIfExistsAsync(this.connectionString, this.partitionHub);
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

        async Task<(EventHubClient, EventHubRuntimeInformation)> EnsureClientAsync(int i, int retries = 2)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
            {
                EntityPath = this.clientHubs[i]
            };
            try
            {
                var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                var runtimeInformation = await client.GetRuntimeInformationAsync();
                return (client, runtimeInformation);
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException) when (retries > 0)
            {
                this.TraceHelper.LogInformation("Creating EventHub {name}", this.clientHubs[i]);
                await EventHubsUtil.EnsureEventHubExistsAsync(this.connectionString, this.clientHubs[i], 32);
            }
            // try again
            return await this.EnsureClientAsync(i, retries - 1);
        }


        async Task EnsureLoadMonitorAsync(int retries = 2)
        {
            // create loadmonitor client
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
            {
                EntityPath = loadMonitorHub,
            };
            try
            {
                this.loadMonitorClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                var runtimeInformation = await this.loadMonitorClient.GetRuntimeInformationAsync();
                return;
            }
            catch (Microsoft.Azure.EventHubs.MessagingEntityNotFoundException) when (retries > 0)
            {
                this.TraceHelper.LogInformation("Creating EventHub {name}", this.loadMonitorHub);
                await EventHubsUtil.EnsureEventHubExistsAsync(this.connectionString, this.loadMonitorHub, 1);
            }
            // try again
            await this.EnsureLoadMonitorAsync(retries - 1);
        }

        public static async Task<List<long>> GetQueuePositionsAsync(string connectionString, string partitionHub)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
            {
                EntityPath = partitionHub,
            };
            var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
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


        public EventHubsSender<PartitionUpdateEvent> GetPartitionSender(int partitionId, byte[] taskHubGuid)
        {
            return this._partitionSenders.GetOrAdd(partitionId, (key) => {
                (EventHubClient client, string id) = this.partitionPartitions[partitionId];
                var partitionSender = client.CreatePartitionSender(id);
                var sender = new EventHubsSender<PartitionUpdateEvent>(
                    this.Host,
                    taskHubGuid,
                    partitionSender,
                    this.TraceHelper);
                this.TraceHelper.LogDebug("Created PartitionSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                return sender;
            });
        }

        public EventHubsClientSender GetClientSender(Guid clientId, byte[] taskHubGuid)
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
                        taskHubGuid,
                        clientId,
                        partitionSenders,
                        this.TraceHelper);
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
                    this.TraceHelper);
                this.TraceHelper.LogDebug("Created LoadMonitorSender {sender} from {clientId}", loadMonitorSender.ClientId, this.loadMonitorClient.ClientId);
                return sender;
            });
        }
    }
}
