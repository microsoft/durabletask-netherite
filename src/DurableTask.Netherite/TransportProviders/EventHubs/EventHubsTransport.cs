// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Newtonsoft.Json;

    /// <summary>
    /// The EventHubs transport implementation.
    /// </summary>
    class EventHubsTransport :
        ITaskHub,
        IEventProcessorFactory,
        TransportAbstraction.ISender
    {
        readonly TransportAbstraction.IHost host;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly CloudStorageAccount cloudStorageAccount;
        readonly EventHubsTraceHelper traceHelper;

        EventProcessorHost eventProcessorHost;
        TransportAbstraction.IClient client;

        TaskhubParameters parameters;
        byte[] taskhubGuid;
        EventHubsConnections connections;

        Task clientEventLoopTask = Task.CompletedTask;
        CancellationTokenSource shutdownSource;
        readonly CloudBlobContainer cloudBlobContainer;
        readonly CloudBlockBlob taskhubParameters;
        readonly CloudBlockBlob partitionScript;
        ScriptedEventProcessorHost scriptedEventProcessorHost;

        public Guid ClientId { get; private set; }

        public EventHubsTransport(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.host = host;
            this.settings = settings;
            this.cloudStorageAccount = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
            string namespaceName = TransportConnectionString.EventHubsNamespaceName(settings.EventHubsConnectionString);
            this.traceHelper = new EventHubsTraceHelper(loggerFactory, settings.TransportLogLevelLimit, this.cloudStorageAccount.Credentials.AccountName, settings.HubName, namespaceName);
            this.ClientId = Guid.NewGuid();
            var blobContainerName = GetContainerName(settings.HubName);
            var cloudBlobClient = this.cloudStorageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskhubParameters = this.cloudBlobContainer.GetBlockBlobReference("taskhubparameters.json");
            this.partitionScript = this.cloudBlobContainer.GetBlockBlobReference("partitionscript.json");
        }

        // these are hardcoded now but we may turn them into settings
        public static string[] PartitionHubs = { "partitions" };
        public static string[] ClientHubs = { "clients0", "clients1", "clients2", "clients3" };
        public static string PartitionConsumerGroup = "$Default";
        public static string ClientConsumerGroup = "$Default";

        static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        async Task<TaskhubParameters> TryLoadExistingTaskhubAsync()
        {
            // try load the taskhub parameters
            try
            {
                var jsonText = await this.taskhubParameters.DownloadTextAsync().ConfigureAwait(false);
                return  JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 404)
            {
                return null;
            }
        }

        async Task<bool> ITaskHub.ExistsAsync()
        {
            var parameters = await this.TryLoadExistingTaskhubAsync().ConfigureAwait(false);
            return (parameters != null && parameters.TaskhubName == this.settings.HubName);
        }

        async Task ITaskHub.CreateAsync()
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync().ConfigureAwait(false);

            if (await this.TryLoadExistingTaskhubAsync().ConfigureAwait(false) != null)
            {
                throw new InvalidOperationException("Cannot create TaskHub: TaskHub already exists");
            }

            long[] startPositions = await EventHubsConnections.GetQueuePositions(this.settings.EventHubsConnectionString, EventHubsTransport.PartitionHubs);

            var taskHubParameters = new TaskhubParameters()
            {
                TaskhubName = this.settings.HubName,
                TaskhubGuid = Guid.NewGuid(),
                CreationTimestamp = DateTime.UtcNow,
                PartitionHubs = EventHubsTransport.PartitionHubs,
                ClientHubs = EventHubsTransport.ClientHubs,
                PartitionConsumerGroup = EventHubsTransport.PartitionConsumerGroup,
                ClientConsumerGroup = EventHubsTransport.ClientConsumerGroup,
                StartPositions = startPositions
            };

            // save the taskhub parameters in a blob
            var jsonText = JsonConvert.SerializeObject(
                taskHubParameters,
                Newtonsoft.Json.Formatting.Indented,
                new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });
            await this.taskhubParameters.UploadTextAsync(jsonText).ConfigureAwait(false);
        }
      
        async Task ITaskHub.DeleteAsync()
        {
            if (await this.taskhubParameters.ExistsAsync().ConfigureAwait(false))
            {
                await BlobUtils.ForceDeleteAsync(this.taskhubParameters).ConfigureAwait(false);
            }

            // todo delete consumption checkpoints
            await this.host.StorageProvider.DeleteAllPartitionStatesAsync().ConfigureAwait(false);
        }

        async Task ITaskHub.StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            // load the taskhub parameters
            var jsonText = await this.taskhubParameters.DownloadTextAsync().ConfigureAwait(false);
            this.parameters = JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
            this.taskhubGuid = this.parameters.TaskhubGuid.ToByteArray();

            // check that we are the correct taskhub!
            if (this.parameters.TaskhubName != this.settings.HubName)
            {
                throw new InvalidOperationException($"The specified taskhub name does not match the task hub name in {this.taskhubParameters.Name}");
            }
          
            this.host.NumberPartitions = (uint) this.parameters.StartPositions.Length;

            this.connections = new EventHubsConnections(this.settings.EventHubsConnectionString, this.parameters.PartitionHubs, this.parameters.ClientHubs)
            {
                Host = host,
                TraceHelper = this.traceHelper,
                UseJsonPackets = this.settings.UseJsonPackets,
            };

            await this.connections.StartAsync(this.parameters.StartPositions.Length);

            this.client = this.host.AddClient(this.ClientId, this.parameters.TaskhubGuid, this);

            this.clientEventLoopTask = Task.Run(this.ClientEventLoop);

            if (PartitionHubs.Length > 1)
            {
                throw new NotSupportedException("Using multiple eventhubs for partitions is not yet supported.");
            }

            string partitionsHub = PartitionHubs[0];

            // Use standard eventProcessor offered by EventHubs or a custom one
            if (this.settings.EventProcessorManagement == "EventHubs")
            {

                this.eventProcessorHost = new EventProcessorHost(
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        this.settings.EventHubsConnectionString,
                        this.settings.StorageConnectionString,
                        this.cloudBlobContainer.Name);

                var processorOptions = new EventProcessorOptions()
                {
                    InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(this.parameters.StartPositions[int.Parse(s)] - 1),
                    MaxBatchSize = 300,
                    PrefetchCount = 500,
                };

                await this.eventProcessorHost.RegisterEventProcessorFactoryAsync(this, processorOptions).ConfigureAwait(false);
            }
            else if (this.settings.EventProcessorManagement.StartsWith("Custom"))
            {
                this.traceHelper.LogWarning($"EventProcessorManagement: {this.settings.EventProcessorManagement}");
                this.scriptedEventProcessorHost = new ScriptedEventProcessorHost(
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        this.settings.EventHubsConnectionString,
                        this.settings.StorageConnectionString,
                        this.cloudBlobContainer.Name,
                        this.host, 
                        this, 
                        this.connections,
                        this.parameters, 
                        this.settings,
                        this.traceHelper,
                        this.settings.WorkerId);

                var thread = new Thread(() => this.scriptedEventProcessorHost.StartEventProcessing(this.settings, this.partitionScript));
                thread.Name = "ScriptedEventProcessorHost";
                thread.Start();
            }
            else
            {
                throw new InvalidOperationException("Unknown EventProcessorManagement setting!");
            }
        }

        async Task ITaskHub.StopAsync(bool isForced)
        {
            this.traceHelper.LogInformation("Shutting down EventHubsBackend");
            this.traceHelper.LogDebug("Stopping client event loop");
            this.shutdownSource.Cancel();
            this.traceHelper.LogDebug("Stopping client");
            await this.client.StopAsync().ConfigureAwait(false);
            this.traceHelper.LogDebug("Unregistering event processor");

            if (this.settings.EventProcessorManagement == "EventHubs")
            {
                await this.eventProcessorHost.UnregisterEventProcessorAsync().ConfigureAwait(false);
            }
            else if (this.settings.EventProcessorManagement.StartsWith("Custom"))
            {
                await this.scriptedEventProcessorHost.StopAsync();
            }
            else
            {
                throw new InvalidOperationException("Unknown EventProcessorManagement setting!");
            }
            this.traceHelper.LogDebug("Closing connections");
            await this.connections.StopAsync().ConfigureAwait(false);
            this.traceHelper.LogInformation("EventHubsBackend shutdown completed");
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext partitionContext)
        {
            var processor = new EventHubsProcessor(this.host, this, this.parameters, partitionContext, this.settings, this.traceHelper);
            return processor;
        }

        void TransportAbstraction.ISender.Submit(Event evt)
        {
            switch (evt)
            {
                case ClientEvent clientEvent:
                    var clientId = clientEvent.ClientId;
                    var clientSender = this.connections.GetClientSender(clientEvent.ClientId, this.taskhubGuid);
                    clientSender.Submit(clientEvent);
                    break;

                case PartitionEvent partitionEvent:
                    var partitionId = partitionEvent.PartitionId;
                    var partitionSender = this.connections.GetPartitionSender((int) partitionId, this.taskhubGuid);
                    partitionSender.Submit(partitionEvent);
                    break;

                default:
                    throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
            }
        }

        async Task ClientEventLoop()
        {
            var clientReceiver = this.connections.CreateClientReceiver(this.ClientId, EventHubsTransport.ClientConsumerGroup);
            var receivedEvents = new List<ClientEvent>();

            byte[] taskHubGuid = this.parameters.TaskhubGuid.ToByteArray();

            while (!this.shutdownSource.IsCancellationRequested)
            {
                IEnumerable<EventData> eventData = await clientReceiver.ReceiveAsync(1000, TimeSpan.FromMinutes(1)).ConfigureAwait(false);

                if (eventData != null)
                {
                    foreach (var ed in eventData)
                    {
                        ClientEvent clientEvent = null;

                        try
                        {
                            Packet.Deserialize(ed.Body, out clientEvent, taskHubGuid);

                            if (clientEvent != null && clientEvent.ClientId == this.ClientId)
                            {
                                receivedEvents.Add(clientEvent);
                            }
                        }
                        catch (Exception)
                        {
                            this.traceHelper.LogError("EventProcessor for Client{clientId} could not deserialize packet #{seqno} ({size} bytes)", Client.GetShortId(this.ClientId), ed.SystemProperties.SequenceNumber, ed.Body.Count);
                            throw;
                        }
                        this.traceHelper.LogDebug("EventProcessor for Client{clientId} received packet #{seqno} ({size} bytes)", Client.GetShortId(this.ClientId), ed.SystemProperties.SequenceNumber, ed.Body.Count);

                    }

                    if (receivedEvents.Count > 0)
                    {
                        foreach (var evt in receivedEvents)
                        {
                            this.client.Process(evt);
                        }
                        receivedEvents.Clear();
                    }
                }
            }
        }
    }
}
