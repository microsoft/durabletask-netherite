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
        readonly string[] partitionHubs;
        readonly string[] clientHubs;
        readonly string loadMonitorHub;

        List<EventHubClient> partitionClients;
        List<EventHubClient> clientClients;
        EventHubClient loadMonitorClient;

        readonly List<(EventHubClient client, string id)> partitionPartitions = new List<(EventHubClient client, string id)>();
        readonly List<(EventHubClient client, string id)> clientPartitions = new List<(EventHubClient client, string id)>();

        public const int NumClientChannels = 2;

        public string Endpoint { get; private set; }
        public DateTime[] CreationTimestamps { get; private set; }

        public ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>> _partitionSenders = new ConcurrentDictionary<int, EventHubsSender<PartitionUpdateEvent>>();
        public ConcurrentDictionary<Guid, EventHubsClientSender> _clientSenders = new ConcurrentDictionary<Guid, EventHubsClientSender>();
        public ConcurrentDictionary<int, LoadMonitorSender> _loadMonitorSenders = new ConcurrentDictionary<int, LoadMonitorSender>();

        public TransportAbstraction.IHost Host { get; set; }
        public EventHubsTraceHelper TraceHelper { get; set; }

        int GetClientBucket(Guid clientId, int index) => (int)((Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) + index) % (uint)this.clientPartitions.Count);

        public EventHubsConnections(
            string connectionString, 
            string[] partitionHubs,
            string[] clientHubs,
            string loadMonitorHub)
        {
            this.connectionString = connectionString;
            this.partitionHubs = partitionHubs;
            this.clientHubs = clientHubs;
            this.loadMonitorHub = loadMonitorHub;
       }

        public async Task StartAsync(TaskhubParameters parameters)
        {
            await Task.WhenAll(this.GetPartitionInformationAsync(), this.GetClientInformationAsync(), this.GetLoadMonitorInformationAsync());

            // check that we are the correct namespace
            if (!string.IsNullOrEmpty(parameters.EventHubsEndpoint)
                && string.Compare(parameters.EventHubsEndpoint, this.Endpoint, StringComparison.InvariantCultureIgnoreCase) != 0)
            {
                throw new InvalidOperationException($"Cannot recover taskhub because the EventHubs namespace does not match."
                    + " To resolve, either connect to the original namespace, delete the taskhub in storage, or use a fresh taskhub name.");
            }

            // check that we are dealing with the original event hubs
            if (parameters.EventHubsCreationTimestamps != null)
            {
                for (int i = 0; i < this.CreationTimestamps.Count(); i++)
                {
                    if (this.CreationTimestamps[i] != parameters.EventHubsCreationTimestamps[i])
                    {
                        throw new InvalidOperationException($"Cannot recover taskhub because the original EventHubs was deleted."
                             + " To resolve, delete the taskhub in storage, or use a fresh taskhub name.");
                    }
                }
            }

            // check that the number of partitions matches. I don't think it's possible for this to fail without prior checks tripping first.
            if (parameters.StartPositions.Length != this.partitionPartitions.Count)
            {
                throw new InvalidOperationException($"Cannot recover taskhub because the number of partitions does not match.");
            }
        }

        public async Task StopAsync()
        {
            IEnumerable<EventHubClient> Clients()
            {
                if (this.partitionClients != null)
                {
                    foreach (var client in this.partitionClients)
                    {
                        yield return client;
                    }
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
                this.Endpoint = connectionStringBuilder.Endpoint.ToString();
            }

            // in parallel, get runtime infos for all the hubs
            var partitionInfos = this.partitionClients.Select((ehClient) => ehClient.GetRuntimeInformationAsync()).ToList();
            await Task.WhenAll(partitionInfos);

            this.CreationTimestamps = partitionInfos.Select(t => t.Result.CreatedAt).ToArray();

            // create a flat list of partition partitions
            for (int i = 0; i < this.partitionHubs.Length; i++)
            {
                foreach (var id in partitionInfos[i].Result.PartitionIds)
                {
                    this.partitionPartitions.Add((this.partitionClients[i], id));
                }
            }
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

        Task GetLoadMonitorInformationAsync()
        {
            // create loadmonitor client
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.connectionString)
            {
                EntityPath = loadMonitorHub,
            };
            this.loadMonitorClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            return Task.CompletedTask;
        }

        public static async Task<(long[], DateTime[], string)> GetPartitionInfo(string connectionString, string[] partitionHubs)
        {
            var connections = new EventHubsConnections(connectionString, partitionHubs, new string[0], null);
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

            return (positions, connections.CreationTimestamps, connections.Endpoint);
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
