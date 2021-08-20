// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
    using DurableTask.Netherite.Faster;

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
        readonly ILogger logger;
        readonly EventHubsTraceHelper traceHelper;

        EventProcessorHost eventProcessorHost;
        EventProcessorHost loadMonitorHost;
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
            this.cloudStorageAccount = CloudStorageAccount.Parse(this.settings.ResolvedStorageConnectionString);
            string namespaceName = TransportConnectionString.EventHubsNamespaceName(settings.ResolvedTransportConnectionString);
            this.logger = EventHubsTraceHelper.CreateLogger(loggerFactory);
            this.traceHelper = new EventHubsTraceHelper(this.logger, settings.TransportLogLevelLimit, null, this.cloudStorageAccount.Credentials.AccountName, settings.HubName, namespaceName);
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
        public static string  LoadMonitorHub = "loadmonitor";
        public static string PartitionConsumerGroup = "$Default";
        public static string ClientConsumerGroup = "$Default";
        public static string LoadMonitorConsumerGroup = "$Default";

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

        async Task<bool> ITaskHub.CreateIfNotExistsAsync()
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync().ConfigureAwait(false);

            // ensure the task hubs exist, creating them if necessary
            var tasks = new List<Task>();
            tasks.Add(EventHubsUtil.EnsureEventHubExistsAsync(this.settings.ResolvedTransportConnectionString, PartitionHubs[0], this.settings.PartitionCount));
            if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
            {
                tasks.Add(EventHubsUtil.EnsureEventHubExistsAsync(this.settings.ResolvedTransportConnectionString, LoadMonitorHub, 1));
            }
            foreach (string taskhub in ClientHubs)
            {
                tasks.Add(EventHubsUtil.EnsureEventHubExistsAsync(this.settings.ResolvedTransportConnectionString, taskhub, 32));
            }
            await Task.WhenAll(tasks);

            // determine the start positions and the creation timestamps
            (long[] startPositions, DateTime[] creationTimestamps, string namespaceEndpoint)
                = await EventHubsConnections.GetPartitionInfo(this.settings.ResolvedTransportConnectionString, EventHubsTransport.PartitionHubs);

            var taskHubParameters = new TaskhubParameters()
            {
                TaskhubName = this.settings.HubName,
                TaskhubGuid = Guid.NewGuid(),
                CreationTimestamp = DateTime.UtcNow,
                StorageFormat = BlobManager.GetStorageFormat(this.settings),
                PartitionHubs = EventHubsTransport.PartitionHubs,
                ClientHubs = EventHubsTransport.ClientHubs,
                PartitionConsumerGroup = EventHubsTransport.PartitionConsumerGroup,
                ClientConsumerGroup = EventHubsTransport.ClientConsumerGroup,
                EventHubsEndpoint = namespaceEndpoint,
                EventHubsCreationTimestamps = creationTimestamps,
                StartPositions = startPositions
            };

            // try to create the taskhub blob
            try
            {
                var jsonText = JsonConvert.SerializeObject(
                    taskHubParameters,
                    Newtonsoft.Json.Formatting.Indented,
                    new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });

                var noOverwrite = AccessCondition.GenerateIfNoneMatchCondition("*");
                await this.taskhubParameters.UploadTextAsync(jsonText, null, noOverwrite, null, null).ConfigureAwait(false);
            }
            catch(StorageException e) when (BlobUtils.BlobAlreadyExists(e))
            {
                // taskhub already exists, possibly because a different node created it faster
                return false;
            }

            // we successfully created the taskhub
            return true;
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

            // check that the storage format is supported
            BlobManager.CheckStorageFormat(this.parameters.StorageFormat, this.settings);

            this.host.NumberPartitions = (uint)this.parameters.StartPositions.Length;
           
            this.connections = new EventHubsConnections(this.settings.ResolvedTransportConnectionString, this.parameters.PartitionHubs, this.parameters.ClientHubs, LoadMonitorHub)
            {
                Host = host,
                TraceHelper = this.traceHelper,
            };

            await this.connections.StartAsync(this.parameters);

            this.client = this.host.AddClient(this.ClientId, this.parameters.TaskhubGuid, this);

            this.clientEventLoopTask = Task.Run(this.ClientEventLoop);

            if (PartitionHubs.Length > 1)
            {
                throw new NotSupportedException("Using multiple eventhubs for partitions is not yet supported.");
            }

            if (this.client == null)
            {
                throw new InvalidOperationException("client must be started before partition hosting is started.");
            }

            string partitionsHub = PartitionHubs[0];
            
            if (this.settings.PartitionManagement != PartitionManagementOptions.ClientOnly)
            {
                if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
                {
                    await Task.WhenAll(StartPartitionHost(), StartLoadMonitorHost()).ConfigureAwait(false);
                }
                else
                {
                    await StartPartitionHost().ConfigureAwait(false);
                }
            }

            async Task StartPartitionHost()
            {
                if (this.settings.PartitionManagement != PartitionManagementOptions.Scripted)
                {
                    this.traceHelper.LogInformation("Registering Partition Host with EventHubs");

                    this.eventProcessorHost = new EventProcessorHost(
                        Guid.NewGuid().ToString(),
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        this.settings.ResolvedTransportConnectionString,
                        this.settings.ResolvedStorageConnectionString,
                        this.cloudBlobContainer.Name);

                    var processorOptions = new EventProcessorOptions()
                    {
                        InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(this.parameters.StartPositions[int.Parse(s)] - 1),
                        MaxBatchSize = 300,
                        PrefetchCount = 500,
                    };

                    await this.eventProcessorHost.RegisterEventProcessorFactoryAsync(
                        new PartitionEventProcessorFactory(this), 
                        processorOptions).ConfigureAwait(false);
                }
                else
                {
                    this.traceHelper.LogInformation($"Starting scripted partition host");
                    this.scriptedEventProcessorHost = new ScriptedEventProcessorHost(
                            partitionsHub,
                            EventHubsTransport.PartitionConsumerGroup,
                            this.settings.ResolvedTransportConnectionString,
                            this.settings.ResolvedStorageConnectionString,
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
            }

            async Task StartLoadMonitorHost()
            {
                this.traceHelper.LogInformation("Registering LoadMonitor Host with EventHubs");

                this.loadMonitorHost = new EventProcessorHost(
                        Guid.NewGuid().ToString(),
                        LoadMonitorHub,
                        LoadMonitorConsumerGroup,
                        this.settings.ResolvedTransportConnectionString,
                        this.settings.ResolvedStorageConnectionString,
                        this.cloudBlobContainer.Name,
                        LoadMonitorHub);

                var processorOptions = new EventProcessorOptions()
                {
                    InitialOffsetProvider = (s) => EventPosition.FromEnqueuedTime(DateTime.UtcNow - TimeSpan.FromSeconds(30)),
                    MaxBatchSize = 500,
                    PrefetchCount = 500,
                };

                await this.loadMonitorHost.RegisterEventProcessorFactoryAsync(
                    new LoadMonitorEventProcessorFactory(this), 
                    processorOptions).ConfigureAwait(false);
            }
        }

        class PartitionEventProcessorFactory : IEventProcessorFactory
        {
            readonly EventHubsTransport transport;

            public PartitionEventProcessorFactory(EventHubsTransport transport)
            {
                this.transport = transport;
            }

            public IEventProcessor CreateEventProcessor(PartitionContext context)
            {
                return new EventHubsProcessor(
                    this.transport.host,
                    this.transport,
                    this.transport.parameters,
                    context,
                    this.transport.settings,
                    this.transport,
                    this.transport.traceHelper,
                    this.transport.shutdownSource.Token);
            }
        }

        class LoadMonitorEventProcessorFactory : IEventProcessorFactory
        {
            readonly EventHubsTransport transport;

            public LoadMonitorEventProcessorFactory(EventHubsTransport transport)
            {
                this.transport = transport;
            }

            public IEventProcessor CreateEventProcessor(PartitionContext context)
            {
                return new LoadMonitorProcessor(
                    this.transport.host,
                    this.transport,
                    this.transport.parameters,
                    context,
                    this.transport.settings,
                    this.transport.traceHelper,
                    this.transport.shutdownSource.Token);
            }
        }

        async Task ITaskHub.StopAsync(bool isForced)
        {
            this.traceHelper.LogInformation("Shutting down EventHubsBackend");
            this.shutdownSource.Cancel(); // initiates shutdown of client and of all partitions

            this.traceHelper.LogDebug("Stopping client");
            await this.client.StopAsync().ConfigureAwait(false);

            if (this.settings.PartitionManagement != PartitionManagementOptions.ClientOnly)
            {
                if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
                {
                    this.traceHelper.LogDebug("Stopping partition and loadmonitor hosts");
                    await Task.WhenAll(
                      this.eventProcessorHost.UnregisterEventProcessorAsync(),
                      this.loadMonitorHost.UnregisterEventProcessorAsync()).ConfigureAwait(false);
                }
                else
                {
                    this.traceHelper.LogDebug("Stopping partition host");
                    await this.eventProcessorHost.UnregisterEventProcessorAsync().ConfigureAwait(false);
                }
            }

            this.traceHelper.LogDebug("Closing connections");
            await this.connections.StopAsync().ConfigureAwait(false);
            this.traceHelper.LogInformation("EventHubsBackend shutdown completed");
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext partitionContext)
        {
            var processor = new EventHubsProcessor(this.host, this, this.parameters, partitionContext, this.settings, this, this.traceHelper, this.shutdownSource.Token);
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

                case LoadMonitorEvent loadMonitorEvent:
                    var loadMonitorSender = this.connections.GetLoadMonitorSender(this.taskhubGuid);
                    loadMonitorSender.Submit(loadMonitorEvent);
                    break;

                default:
                    throw new InvalidCastException($"unknown type of event: {evt.GetType().FullName}");
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
