// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

    class EventHubsConnections
    {
        readonly string connectionString;
        readonly string[] partitionHubs;
        readonly string[] clientHubs;

        List<EventHubClient> partitionClients;
        List<EventHubClient> clientClients;
        readonly List<(EventHubClient client, string id)> partitionPartitions = new List<(EventHubClient client, string id)>();
        readonly List<(EventHubClient client, string id)> clientPartitions = new List<(EventHubClient client, string id)>();

        public ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>> _partitionSenders = new ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>>();
        public ConcurrentDictionary<Guid, EventHubsSender<ClientEvent>> _clientSenders = new ConcurrentDictionary<Guid, EventHubsSender<ClientEvent>>();

        public TransportAbstraction.IHost Host { get; set; }
        public NetheriteOrchestrationServiceSettings.JsonPacketUse UseJsonPackets { get; set; }
        public EventHubsTraceHelper TraceHelper { get; set; }

        int GetClientBucket(Guid clientId) => (int)(Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % (uint)this.clientClients.Count);

        public EventHubsConnections(
            string connectionString, 
            string[] partitionHubs,
            string[] clientHubs)
        {
            this.connectionString = connectionString;
            this.partitionHubs = partitionHubs;
            this.clientHubs = clientHubs;
        }

        public async Task StartAsync(int numberPartitions)
        {
            await Task.WhenAll(this.GetPartitionInformationAsync(), this.GetClientInformationAsync());

            if (numberPartitions != this.partitionPartitions.Count)
            {
                throw new InvalidOperationException("The number of partitions in the specified EventHubs namespace does not match the number of partitions in the TaskHub");
            }
        }

        public async Task StopAsync()
        {
            var closePartitionClients = this.partitionClients.Select(c => c.CloseAsync()).ToList();
            var closeClientClients = this.partitionClients.Select(c => c.CloseAsync()).ToList();
            await Task.WhenAll(closePartitionClients);
            await Task.WhenAll(closeClientClients);
        }

        async Task GetPartitionInformationAsync()
        {
            // create partition clients
            this.partitionClients = new List<EventHubClient>();
            for (int i = 0; i < this.partitionHubs.Length; i++)
            {
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
                {
                    EntityPath = this.partitionHubs[i]
                };
                var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                this.partitionClients.Add(client);
            }

            // in parallel, get runtime infos for all the hubs
            var partitionInfos = this.partitionClients.Select((ehClient) => ehClient.GetRuntimeInformationAsync()).ToList();

            // create a flat list of partition partitions
            await Task.WhenAll(partitionInfos);
            for (int i = 0; i < this.partitionHubs.Length; i++)
            {
                foreach (var id in partitionInfos[i].Result.PartitionIds)
                {
                    this.partitionPartitions.Add((this.partitionClients[i], id));
                }
            }

            // validate the total number of partitions

        }

        async Task GetClientInformationAsync()
        {
            // create client clients
            this.clientClients = new List<EventHubClient>();
            for (int i = 0; i < this.clientHubs.Length; i++)
            {
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
                {
                    EntityPath = this.clientHubs[i]
                };
                var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                this.clientClients.Add(client);
            }

            // in parallel, get runtime infos for all the hubs
            var clientInfos = this.clientClients.Select((ehClient) => ehClient.GetRuntimeInformationAsync()).ToList();

            // create a flat list of client partitions
            await Task.WhenAll(clientInfos);
            for (int i = 0; i < this.clientHubs.Length; i++)
            {
                foreach (var id in clientInfos[i].Result.PartitionIds)
                {
                    this.clientPartitions.Add((this.clientClients[i], id));
                }
            }
        }

        public static async Task<long[]> GetQueuePositions(string connectionString, string[] partitionHubs)
        {
            var connections = new EventHubsConnections(connectionString, partitionHubs, new string[0]);
            await connections.GetPartitionInformationAsync();

            var numberPartitions = connections.partitionPartitions.Count;

            var positions = new long[numberPartitions];

            var infoTasks = connections.partitionPartitions
                .Select(x => x.client.GetPartitionRuntimeInformationAsync(x.id)).ToList();

            await Task.WhenAll(infoTasks);

            for (int i = 0; i < numberPartitions; i++)
            {
                var queueInfo = await infoTasks[i].ConfigureAwait(false);
                positions[i] = queueInfo.LastEnqueuedSequenceNumber + 1;
            }

            await connections.StopAsync();

            return positions;
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

        public PartitionReceiver CreateClientReceiver(Guid clientId, string consumerGroupName)
        {
            int clientBucket = this.GetClientBucket(clientId);
            (EventHubClient client, string id) = this.clientPartitions[clientBucket];
            var clientReceiver = client.CreateReceiver(consumerGroupName, id, EventPosition.FromEnd());
            this.TraceHelper.LogDebug("Created Client {clientId} PartitionReceiver {receiver} from {clientId}", clientId, clientReceiver.ClientId, client.ClientId);
            return clientReceiver;
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
                    this.TraceHelper,
                    this.UseJsonPackets >= NetheriteOrchestrationServiceSettings.JsonPacketUse.ForAll);
                this.TraceHelper.LogDebug("Created PartitionSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                return sender;
            });
        }

        public EventHubsSender<ClientEvent> GetClientSender(Guid clientId, byte[] taskHubGuid)
        {
            return this._clientSenders.GetOrAdd(clientId, (key) =>
            {
                int clientBucket = this.GetClientBucket(clientId);
                (EventHubClient client, string id) = this.clientPartitions[clientBucket];
                var partitionSender = client.CreatePartitionSender(id);
                var sender = new EventHubsSender<ClientEvent>(
                    this.Host,
                    taskHubGuid,
                    partitionSender,
                    this.TraceHelper,
                    this.UseJsonPackets >= NetheriteOrchestrationServiceSettings.JsonPacketUse.ForClients);
                this.TraceHelper.LogDebug("Created ClientSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                return sender;
            });
        }
     }
}
