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
    using System.Threading.Channels;

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
        bool hasWorkers;

        TaskhubParameters parameters;
        byte[] taskhubGuid;
        EventHubsConnections connections;

        Task[] clientReceiveLoops;
        Task clientProcessTask;
        Task[] clientConnectionsEstablished;

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

        // the path prefix is used to prevent some issues (races, partial deletions) when recreating a taskhub of the same name
        // since it is a rare circumstance, taking six characters of the Guid is unique enough
        public static string TaskhubPathPrefix(Guid taskhubGuid) => $"{taskhubGuid.ToString()}/";

        static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        async Task<TaskhubParameters> TryLoadExistingTaskhubAsync()
        {
            // try load the taskhub parameters
            try
            {
                var jsonText = await this.taskhubParameters.DownloadTextAsync();
                return  JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 404)
            {
                return null;
            }
        }

        async Task<bool> ExistsAsync()
        {
            var parameters = await this.TryLoadExistingTaskhubAsync();
            return (parameters != null && parameters.TaskhubName == this.settings.HubName);
        }

        async Task<bool> CreateIfNotExistsAsync()
        {
            bool containerCreated = await this.cloudBlobContainer.CreateIfNotExistsAsync();
            if (containerCreated)
            {
                this.traceHelper.LogInformation("Created new blob container at {container}", this.cloudBlobContainer.Uri);
            }
            else
            {
                this.traceHelper.LogInformation("Using existing blob container at {container}", this.cloudBlobContainer.Uri);
            }

            // ensure the event hubs exist, creating them if necessary
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

            this.traceHelper.LogInformation("Confirmed eventhubs positions=[{positions}] endpoint={endpoint}", string.Join(",", startPositions.Select(x => $"#{x}")), namespaceEndpoint);

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
                await this.taskhubParameters.UploadTextAsync(jsonText, null, noOverwrite, null, null);
                this.traceHelper.LogInformation("Created new taskhub");
            }
            catch (StorageException e) when (BlobUtils.BlobAlreadyExists(e))
            {
                // taskhub already exists, possibly because a different node created it faster
                this.traceHelper.LogInformation("Confirmed existing taskhub");

                return false;
            }

            // we successfully created the taskhub
            return true;
        }

        async Task DeleteAsync()
        {
            var parameters = await this.TryLoadExistingTaskhubAsync();

            if (parameters != null)
            {           
                // first, delete the parameters file which deletes the taskhub logically
                await BlobUtils.ForceDeleteAsync(this.taskhubParameters);

                // delete all the files/blobs in the directory/container that represents this taskhub
                // If this does not complete successfully, some garbage may be left behind.
                await this.host.StorageProvider.DeleteTaskhubAsync(TaskhubPathPrefix(parameters.TaskhubGuid));
            }
        }

        async Task StartClientAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            // load the taskhub parameters
            try
            {
                var jsonText = await this.taskhubParameters.DownloadTextAsync();
                this.parameters = JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
                this.taskhubGuid = this.parameters.TaskhubGuid.ToByteArray();
            }
            catch(StorageException e) when (e.RequestInformation.HttpStatusCode == (int) System.Net.HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException($"The specified taskhub does not exist (TaskHub={this.settings.HubName}, StorageConnectionName={this.settings.StorageConnectionName}, EventHubsConnectionName={this.settings.EventHubsConnectionName})");
            }

            // check that we are the correct taskhub!
            if (this.parameters.TaskhubName != this.settings.HubName)
            {
                throw new InvalidOperationException($"The specified taskhub name does not match the task hub name in {this.taskhubParameters.Name}");
            }

            // check that the storage format is supported
            BlobManager.CheckStorageFormat(this.parameters.StorageFormat, this.settings);

            this.host.NumberPartitions = (uint)this.parameters.StartPositions.Length;
            this.host.PathPrefix = TaskhubPathPrefix(this.parameters.TaskhubGuid);
           
            this.connections = new EventHubsConnections(this.settings.ResolvedTransportConnectionString, this.parameters.PartitionHubs, this.parameters.ClientHubs, LoadMonitorHub)
            {
                Host = host,
                TraceHelper = this.traceHelper,
            };

            await this.connections.StartAsync(this.parameters);

            this.client = this.host.AddClient(this.ClientId, this.parameters.TaskhubGuid, this);

            var channel = Channel.CreateBounded<ClientEvent>(new BoundedChannelOptions(500)
            {
                SingleReader = true,
                AllowSynchronousContinuations = true
            });

            var clientReceivers = this.connections.CreateClientReceivers(this.ClientId, EventHubsTransport.ClientConsumerGroup);

            this.clientConnectionsEstablished = Enumerable
                .Range(0, EventHubsConnections.NumClientChannels)
                .Select(i => this.ClientEstablishConnectionAsync(i, clientReceivers[i]))
                .ToArray();
       
            this.clientReceiveLoops = Enumerable
                .Range(0, EventHubsConnections.NumClientChannels)
                .Select(i => this.ClientReceiveLoopAsync(i, clientReceivers[i], channel.Writer))
                .ToArray();

            this.clientProcessTask = this.ClientProcessLoopAsync(channel.Reader);

            // we must wait for the client receive connections to be established before continuing
            // otherwise, we may miss messages that are sent before the client receiver establishes the receive position
            await Task.WhenAll(this.clientConnectionsEstablished);
        }

        async Task StartWorkersAsync()
        {
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
                this.hasWorkers = true;

                if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
                {
                    await Task.WhenAll(StartPartitionHost(), StartLoadMonitorHost());
                }
                else
                {
                    await StartPartitionHost();
                }
            }

            async Task StartPartitionHost()
            {
                if (this.settings.PartitionManagement != PartitionManagementOptions.Scripted)
                {
                    string initialOffsets = string.Join(",", this.parameters.StartPositions.Select(x => $"#{x}"));
                    this.traceHelper.LogInformation($"Registering Partition Host with EventHubs, initial offsets {initialOffsets}");

                    this.eventProcessorHost = new EventProcessorHost(
                        Guid.NewGuid().ToString(),
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        this.settings.ResolvedTransportConnectionString,
                        this.settings.ResolvedStorageConnectionString,
                        this.cloudBlobContainer.Name,
                        $"{TaskhubPathPrefix(this.parameters.TaskhubGuid)}eh-checkpoints/{(PartitionHubs[0])}");

                    var processorOptions = new EventProcessorOptions()
                    {
                        InitialOffsetProvider = (s) => {
                            var pos = this.parameters.StartPositions[int.Parse(s)];
                            if (pos > 0)
                            {
                                // to work around bug in EH, ask for the most recently received packet to be delivered again
                                this.traceHelper.LogDebug("InitialOffsetProvider: partitions/{partitionId} EventPosition.FromSequenceNumber({offset}, inclusive:true)", s, pos - 1);
                                return EventPosition.FromSequenceNumber(pos - 1, inclusive: true);
                            }
                            else
                            {
                                this.traceHelper.LogDebug("InitialOffsetProvider: partitions/{partitionId} EventPosition.FromStart()", s);
                                return EventPosition.FromStart();
                            }
                        },
                        MaxBatchSize = 300,
                        PrefetchCount = 500,
                    };

                    await this.eventProcessorHost.RegisterEventProcessorFactoryAsync(
                        new PartitionEventProcessorFactory(this), 
                        processorOptions);

                    this.traceHelper.LogInformation($"Partition Host started");
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

                    var thread = TrackedThreads.MakeTrackedThread(() => this.scriptedEventProcessorHost.StartEventProcessing(this.settings, this.partitionScript), "ScriptedEventProcessorHost");
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
                        $"{TaskhubPathPrefix(this.parameters.TaskhubGuid)}eh-checkpoints/{LoadMonitorHub}");

                var processorOptions = new EventProcessorOptions()
                {
                    InitialOffsetProvider = (s) => EventPosition.FromEnqueuedTime(DateTime.UtcNow - TimeSpan.FromSeconds(30)),
                    MaxBatchSize = 500,
                    PrefetchCount = 500,
                };

                await this.loadMonitorHost.RegisterEventProcessorFactoryAsync(
                    new LoadMonitorEventProcessorFactory(this), 
                    processorOptions);
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

        async Task StopAsync()
        {
            this.traceHelper.LogInformation("Shutting down EventHubsBackend");
            this.shutdownSource.Cancel(); // initiates shutdown of client and of all partitions

            this.traceHelper.LogDebug("Stopping client");
            await this.client.StopAsync();

            if (this.hasWorkers)
            {
                if (ActivityScheduling.RequiresLoadMonitor(this.settings.ActivityScheduler))
                {
                    this.traceHelper.LogDebug("Stopping partition and loadmonitor hosts");
                    await Task.WhenAll(
                      this.StopPartitionHost(),
                      this.loadMonitorHost.UnregisterEventProcessorAsync());
                }
                else
                {
                    this.traceHelper.LogDebug("Stopping partition host");
                    await this.eventProcessorHost.UnregisterEventProcessorAsync();
                }
            }

            this.traceHelper.LogDebug("Stopping client process loop");
            await this.clientProcessTask;

            this.traceHelper.LogDebug("Closing connections");
            await this.connections.StopAsync();

            this.traceHelper.LogInformation("EventHubsBackend shutdown completed");
        }

        Task StopPartitionHost()
        {
            if (this.settings.PartitionManagement != PartitionManagementOptions.Scripted)
            {
                return this.eventProcessorHost.UnregisterEventProcessorAsync();
            }
            else
            {
                return this.scriptedEventProcessorHost.StopAsync();
            }   
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

        async Task ClientEstablishConnectionAsync(int index, PartitionReceiver receiver)
        {
            try
            {
                this.traceHelper.LogDebug("Client{clientId}.ch{index} establishing connection", Client.GetShortId(this.ClientId), index);
                // receive a dummy packet to establish connection
                // (the packet, if any, cannot be for this receiver because it is fresh)
                await receiver.ReceiveAsync(1, TimeSpan.FromMilliseconds(1));
                this.traceHelper.LogDebug("Client{clientId}.ch{index} connection established", Client.GetShortId(this.ClientId), index);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("Client{clientId}.ch{index} could not connect: {exception}", Client.GetShortId(this.ClientId), index, exception);
                throw;
            }
        }

        async Task ClientReceiveLoopAsync(int index, PartitionReceiver receiver, ChannelWriter<ClientEvent> channelWriter)
        {
            try
            {
                byte[] taskHubGuid = this.parameters.TaskhubGuid.ToByteArray();
                TimeSpan longPollingInterval = TimeSpan.FromMinutes(1);

                await this.clientConnectionsEstablished[index];

                while (!this.shutdownSource.IsCancellationRequested)
                {
                    this.traceHelper.LogTrace("Client{clientId}.ch{index} waiting for new packets", Client.GetShortId(this.ClientId), index);
 
                    IEnumerable<EventData> eventData = await receiver.ReceiveAsync(1000, longPollingInterval);

                    if (eventData != null)
                    {
                        foreach (var ed in eventData)
                        {
                            this.shutdownSource.Token.ThrowIfCancellationRequested();
                            ClientEvent clientEvent = null;

                            try
                            {
                                this.traceHelper.LogDebug("Client{clientId}.ch{index} received packet #{seqno} ({size} bytes)", Client.GetShortId(this.ClientId), index, ed.SystemProperties.SequenceNumber, ed.Body.Count);
                                Packet.Deserialize(ed.Body, out clientEvent, taskHubGuid);
                                if (clientEvent != null && clientEvent.ClientId == this.ClientId)
                                {
                                    this.traceHelper.LogTrace("Client{clientId}.ch{index} receiving event {evt} id={eventId}]", Client.GetShortId(this.ClientId), index, clientEvent, clientEvent.EventIdString);
                                    await channelWriter.WriteAsync(clientEvent, this.shutdownSource.Token);
                                }
                            }
                            catch (Exception)
                            {
                                this.traceHelper.LogError("Client{clientId}.ch{index} could not deserialize packet #{seqno} ({size} bytes)", Client.GetShortId(this.ClientId), index, ed.SystemProperties.SequenceNumber, ed.Body.Count);
                            }
                        }
                    }
                    else
                    {
                        this.traceHelper.LogTrace("Client{clientId}.ch{index} no new packets for last {longPollingInterval}", Client.GetShortId(this.ClientId), index, longPollingInterval);
                    }
                }
            }
            catch (OperationCanceledException) when (this.shutdownSource.IsCancellationRequested)
            {
                // normal during shutdown
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("Client{clientId}.ch{index} event processing exception: {exception}", Client.GetShortId(this.ClientId), index, exception);
            }
            finally
            {
                this.traceHelper.LogInformation("Client{clientId}.ch{index} event processing terminated", Client.GetShortId(this.ClientId), index);
            }
        }

        async Task ClientProcessLoopAsync(ChannelReader<ClientEvent> channelReader)
        {
            try
            {
                while (!this.shutdownSource.IsCancellationRequested)
                {
                    var clientEvent = await channelReader.ReadAsync(this.shutdownSource.Token);
                    this.client.Process(clientEvent);
                }
            }
            catch(OperationCanceledException) when (this.shutdownSource.IsCancellationRequested)
            {
                // normal during shutdown
            }
            catch(Exception exception)
            {
                this.traceHelper.LogError("Client{clientId} event processing exception: {exception}", Client.GetShortId(this.ClientId), exception);
            }
            finally
            {
                this.traceHelper.LogInformation("Client{clientId} event processing terminated", Client.GetShortId(this.ClientId));
            }
        }

        async Task<bool> ITaskHub.ExistsAsync()
        {
            try
            {
                this.traceHelper.LogDebug("ITaskHub.ExistsAsync called");
                bool result = await this.ExistsAsync();
                this.traceHelper.LogDebug("ITaskHub.ExistsAsync returned {result}", result);
                return result;
            }
            catch (Exception e)
            {
                this.traceHelper.LogError("ITaskHub.ExistsAsync failed with exception: {exception}", e);
                throw;
            }
        }

        async Task<bool> ITaskHub.CreateIfNotExistsAsync()
        {
            try
            {
                this.traceHelper.LogDebug("ITaskHub.CreateIfNotExistsAsync called");
                bool result = await this.CreateIfNotExistsAsync();
                this.traceHelper.LogDebug("ITaskHub.CreateIfNotExistsAsync returned {result}", result);
                return result;
            }
            catch (Exception e)
            {
                this.traceHelper.LogError("ITaskHub.CreateIfNotExistsAsync failed with exception: {exception}", e);
                throw;
            }
        }

        async Task ITaskHub.DeleteAsync()
        {
            try
            {
                this.traceHelper.LogDebug("ITaskHub.DeleteAsync called");
                await this.DeleteAsync();
                this.traceHelper.LogDebug("ITaskHub.DeleteAsync returned");
            }
            catch (Exception e)
            {
                this.traceHelper.LogError("ITaskHub.DeleteAsync failed with exception: {exception}", e);
                throw;
            }
        }

        async Task ITaskHub.StartClientAsync()
        {
            try
            {
                this.traceHelper.LogDebug("ITaskHub.StartClientAsync called");
                await this.StartClientAsync();
                this.traceHelper.LogDebug("ITaskHub.StartClientAsync returned");
            }
            catch (Exception e)
            {
                this.traceHelper.LogError("ITaskHub.StartClientAsync failed with exception: {exception}", e);
                throw;
            }
        }

        async Task ITaskHub.StartWorkersAsync()
        {
            try
            {
                this.traceHelper.LogDebug("ITaskHub.StartWorkersAsync called");
                await this.StartWorkersAsync();
                this.traceHelper.LogDebug("ITaskHub.StartWorkersAsync returned");
            }
            catch (Exception e)
            {
                this.traceHelper.LogError("ITaskHub.StartWorkersAsync failed with exception: {exception}", e);
                throw;
            }
        }

        async Task ITaskHub.StopAsync()
        {
            try
            {
                this.traceHelper.LogDebug("ITaskHub.StopAsync called");
                await this.StopAsync();
                this.traceHelper.LogDebug("ITaskHub.StopAsync returned");
            }
            catch (Exception e)
            {
                this.traceHelper.LogError("ITaskHub.StopAsync failed with exception: {exception}", e);
                throw;
            }
        }
    }
}
