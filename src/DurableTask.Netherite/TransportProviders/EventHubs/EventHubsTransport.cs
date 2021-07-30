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
    using System.Linq;

    /// <summary>
    /// The EventHubs transport implementation.
    /// </summary>
    class EventHubsTransport :
        ITaskHub,
        TransportAbstraction.ISender
    {
        readonly TransportAbstraction.IHost host;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly CloudStorageAccount cloudStorageAccount;
        readonly ILogger logger;
        readonly EventHubsTraceHelper traceHelper;

        EventProcessorHost partitionHost;
        EventProcessorHost workerHost;
        TransportAbstraction.IClient client;
        TransportAbstraction.IWorker worker;

        TaskhubParameters parameters;
        byte[] taskhubGuid;
        EventHubsConnections connections;

        Task clientEventLoopTask = Task.CompletedTask;
        CancellationTokenSource shutdownSource;
        readonly CloudBlobContainer cloudBlobContainer;
        readonly CloudBlockBlob taskhubParameters;
        readonly CloudBlockBlob partitionScript;
        ScriptedEventProcessorHost scriptedEventProcessorHost;

        public Guid HostId { get; private set; }

        public EventHubsTransport(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.host = host;
            this.settings = settings;
            this.cloudStorageAccount = CloudStorageAccount.Parse(this.settings.ResolvedStorageConnectionString);
            string namespaceName = TransportConnectionString.EventHubsNamespaceName(settings.ResolvedTransportConnectionString);
            this.logger = EventHubsTraceHelper.CreateLogger(loggerFactory);
            this.traceHelper = new EventHubsTraceHelper(this.logger, settings.TransportLogLevelLimit, null, this.cloudStorageAccount.Credentials.AccountName, settings.HubName, namespaceName);
            this.HostId = Guid.NewGuid();
            var blobContainerName = GetContainerName(settings.HubName);
            var cloudBlobClient = this.cloudStorageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskhubParameters = this.cloudBlobContainer.GetBlockBlobReference("taskhubparameters.json");
            this.partitionScript = this.cloudBlobContainer.GetBlockBlobReference("partitionscript.json");
        }

        // these are hardcoded now but we may turn them into settings
        public static string[] PartitionHubs = { "partitions" };
        public static string[] ClientHubs = { "clients0", "clients1", "clients2", "clients3" };
        public static string[] WorkerHubs = { "globaltasks" };
        public static string PartitionConsumerGroup = "$Default";
        public static string WorkersConsumerGroup = "$Default";
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

        async Task<bool> ITaskHub.CreateIfNotExistsAsync()
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync().ConfigureAwait(false);

            // ensure the task hubs exist, creating them if necessary
            var tasks = new List<Task>();
            tasks.Add(EventHubsUtil.EnsureEventHubExistsAsync(this.settings.ResolvedTransportConnectionString, PartitionHubs[0], this.settings.PartitionCount));
            foreach (string taskhub in ClientHubs)
            {
                tasks.Add(EventHubsUtil.EnsureEventHubExistsAsync(this.settings.ResolvedTransportConnectionString, taskhub, 32));
            }
            foreach (string taskhub in WorkerHubs)
            {
                tasks.Add(EventHubsUtil.EnsureEventHubExistsAsync(this.settings.ResolvedTransportConnectionString, taskhub, 1));
            }
            await Task.WhenAll(tasks);

            // determine the start positions and the creation timestamps
            (long[] startPositions, long[] workerStartPositions, DateTime[] creationTimestamps, string namespaceEndpoint)
                = await EventHubsConnections.GetPartitionInfo(this.settings.ResolvedTransportConnectionString, EventHubsTransport.PartitionHubs, EventHubsTransport.WorkerHubs);

            var taskHubParameters = new TaskhubParameters()
            {
                TaskhubName = this.settings.HubName,
                TaskhubGuid = Guid.NewGuid(),
                CreationTimestamp = DateTime.UtcNow,
                StorageFormat = BlobManager.GetStorageFormat(this.settings),
                PartitionHubs = EventHubsTransport.PartitionHubs,
                ClientHubs = EventHubsTransport.ClientHubs,
                WorkerHubs = EventHubsTransport.WorkerHubs,
                PartitionConsumerGroup = EventHubsTransport.PartitionConsumerGroup,
                ClientConsumerGroup = EventHubsTransport.ClientConsumerGroup,
                EventHubsEndpoint = namespaceEndpoint,
                EventHubsCreationTimestamps = creationTimestamps,
                StartPositions = startPositions,
                WorkerStartPositions = workerStartPositions
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

            this.connections = new EventHubsConnections(this.settings.ResolvedTransportConnectionString, this.parameters.PartitionHubs, this.parameters.ClientHubs, this.parameters.WorkerHubs)
            {
                Host = host,
                TraceHelper = this.traceHelper,
            };

            await this.connections.StartAsync(this.parameters);

            this.client = this.host.AddClient(this.HostId, this.parameters.TaskhubGuid, this);

            if (this.settings.PartitionManagement != PartitionManagementOptions.ClientOnly)
            {
                this.worker = this.host.AddWorker(this.HostId, this.parameters.TaskhubGuid, this);
            }

            this.clientEventLoopTask = Task.Run(this.ClientEventLoop);

            if (PartitionHubs.Length > 1)
            {
                throw new NotSupportedException("Using multiple eventhubs for partitions is not yet supported.");
            }

            string partitionsHub = PartitionHubs[0];
            string workersHub = WorkerHubs[0];

            if (this.settings.PartitionManagement != PartitionManagementOptions.ClientOnly)
            {
                //await Task.WhenAll(StartPartitionHost(), StartWorkerHost()).ConfigureAwait(false);
                await StartWorkerHost();
                await StartPartitionHost();
            }

            async Task StartPartitionHost()
            {
                if (this.settings.PartitionManagement != PartitionManagementOptions.Scripted)
                {
                    this.traceHelper.LogInformation("Registering Partition Host with EventHubs");

                    this.partitionHost = new EventProcessorHost(
                        this.HostId.ToString("N"),
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        this.settings.ResolvedTransportConnectionString,
                        this.settings.ResolvedStorageConnectionString,
                        this.cloudBlobContainer.Name,
                        partitionsHub);

                    var processorOptions = new EventProcessorOptions()
                    {
                        InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(this.parameters.StartPositions[int.Parse(s)] - 1),
                        MaxBatchSize = 300,
                        PrefetchCount = 500,
                    };

                    await this.partitionHost.RegisterEventProcessorFactoryAsync(new PartitionEventProcessorFactory(this), processorOptions).ConfigureAwait(false);
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

            async Task StartWorkerHost()
            {
                this.traceHelper.LogInformation("Registering Worker Host with EventHubs");

                this.workerHost = new EventProcessorHost(
                        this.HostId.ToString(),
                        workersHub,
                        EventHubsTransport.WorkersConsumerGroup,
                        this.settings.ResolvedTransportConnectionString,
                        this.settings.ResolvedStorageConnectionString,
                        this.cloudBlobContainer.Name,
                        workersHub);

                var processorOptions = new EventProcessorOptions()
                {
                    InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(this.parameters.WorkerStartPositions[int.Parse(s)] - 1),
                    MaxBatchSize = 100,
                    PrefetchCount = 20,
                };

                await this.workerHost.RegisterEventProcessorFactoryAsync(new WorkerEventProcessorFactory(this), processorOptions).ConfigureAwait(false);
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
                return new PartitionProcessor(
                    this.transport.host,
                    this.transport,
                    this.transport.parameters,
                    context,
                    this.transport.settings,
                    this.transport.traceHelper,
                    this.transport.shutdownSource.Token);
            }
        }

        class WorkerEventProcessorFactory : IEventProcessorFactory
        {
            readonly EventHubsTransport transport;

            public WorkerEventProcessorFactory(EventHubsTransport transport)
            {
                this.transport = transport;
            }

            public IEventProcessor CreateEventProcessor(PartitionContext context)
            {
                return new WorkerProcessor(
                    this.transport.host,
                    this.transport.worker,
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

            this.traceHelper.LogDebug("Stopping client and worker");
            var clientStopTask = this.client.StopAsync();
            var workerStopTask = this.worker?.StopAsync() ?? Task.CompletedTask;
            await Task.WhenAll(clientStopTask, workerStopTask).ConfigureAwait(false);

            switch (this.settings.PartitionManagement)
            {
                case PartitionManagementOptions.EventProcessorHost:
                    {
                        this.traceHelper.LogDebug("Stopping partition and worker hosts");
                        await Task.WhenAll(
                          this.partitionHost.UnregisterEventProcessorAsync(),
                          this.workerHost.UnregisterEventProcessorAsync()).ConfigureAwait(false);
                        break;
                    }

                case PartitionManagementOptions.Scripted:
                    {
                        this.traceHelper.LogDebug("Stopping partition and worker hosts");
                        await Task.WhenAll(
                          this.scriptedEventProcessorHost.StopAsync(),
                          this.workerHost.UnregisterEventProcessorAsync()).ConfigureAwait(false);
                        break;
                    }

                case PartitionManagementOptions.ClientOnly:
                    {
                        break;
                    }
            }

            this.traceHelper.LogDebug("Closing connections");
            await this.connections.StopAsync().ConfigureAwait(false);
            this.traceHelper.LogInformation("EventHubsBackend shutdown completed");
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

                case WorkerEvent workerEvent:
                    var workerSender = this.connections.GetWorkerSender(this.taskhubGuid, this.worker);
                    workerSender.Submit(workerEvent);
                    break;

                default:
                    throw new InvalidCastException("could not cast to a supported Event category");
            }
        }

        async Task ClientEventLoop()
        {
            var clientReceiver = this.connections.CreateClientReceiver(this.HostId, EventHubsTransport.ClientConsumerGroup);
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

                            if (clientEvent != null && clientEvent.ClientId == this.HostId)
                            {
                                receivedEvents.Add(clientEvent);
                            }
                        }
                        catch (Exception)
                        {
                            this.traceHelper.LogError("EventProcessor for Client{clientId} could not deserialize packet #{seqno} ({size} bytes)", Client.GetShortId(this.HostId), ed.SystemProperties.SequenceNumber, ed.Body.Count);
                            throw;
                        }
                        this.traceHelper.LogDebug("EventProcessor for Client{clientId} received packet #{seqno} ({size} bytes)", Client.GetShortId(this.HostId), ed.SystemProperties.SequenceNumber, ed.Body.Count);

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
