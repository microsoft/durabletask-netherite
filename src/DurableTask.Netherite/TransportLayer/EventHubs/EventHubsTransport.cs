﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
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
    using DurableTask.Netherite.Abstractions;
    using System.Diagnostics;

    /// <summary>
    /// The EventHubs transport implementation.
    /// </summary>
    class EventHubsTransport :
        ITransportLayer,
        IEventProcessorFactory,
        TransportAbstraction.ISender
    {
        readonly TransportAbstraction.IHost host;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly ILogger logger;
        readonly EventHubsTraceHelper traceHelper;
        readonly IStorageLayer storage;
        readonly string consumerGroup;

        string shortClientId;

        EventProcessorHost eventProcessorHost;
        EventProcessorHost loadMonitorHost;
        TransportAbstraction.IClient client;
        bool hasWorkers;

        TaskhubParameters parameters;
        string pathPrefix;
        byte[] taskhubGuid;
        EventHubsConnections connections;

        Task[] clientReceiveLoops;
        Task clientProcessTask;
        Task[] clientConnectionsEstablished;

        CancellationTokenSource shutdownSource;
        CloudBlobContainer cloudBlobContainer;
        CloudBlockBlob partitionScript;
        ScriptedEventProcessorHost scriptedEventProcessorHost;

        Offsets offsets;

        int shutdownTriggered;

        public Guid ClientId { get; private set; }

        public string Fingerprint { get; private set; }

        public bool FatalExceptionObserved { get; private set; }
    
        public EventHubsTransport(TransportAbstraction.IHost host, NetheriteOrchestrationServiceSettings settings, IStorageLayer storage, ILoggerFactory loggerFactory)
        {
            if (storage is MemoryStorageLayer)
            {
                throw new NetheriteConfigurationException($"Configuration error: in-memory storage cannot be used together with a real event hubs namespace");
            }

            this.host = host;
            this.settings = settings;
            this.storage = storage;
            string namespaceName = settings.EventHubsConnection.ResourceName;
            this.logger = EventHubsTraceHelper.CreateLogger(loggerFactory);
            this.traceHelper = new EventHubsTraceHelper(this.logger, settings.TransportLogLevelLimit, null, settings.StorageAccountName, settings.HubName, namespaceName);
            this.consumerGroup = settings.UseSeparateConsumerGroups ? settings.HubName : "$Default";
            this.ClientId = Guid.NewGuid();
            this.shortClientId = Client.GetShortId(this.ClientId);
        }

        // these are hardcoded now but we may turn them into settings
        public static string PartitionHub = "partitions";
        public static string[] ClientHubs = { "clients0", "clients1", "clients2", "clients3" };
        public static string  LoadMonitorHub = "loadmonitor";

        async Task<TaskhubParameters> ITransportLayer.StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            this.parameters = await this.storage.TryLoadTaskhubAsync(throwIfNotFound: true);

            // check that we are the correct taskhub!
            if (this.parameters.TaskhubName != this.settings.HubName)
            {
                throw new NetheriteConfigurationException($"The specified taskhub name does not match the task hub name in storage");
            }

            this.taskhubGuid = this.parameters.TaskhubGuid.ToByteArray();
            (string containerName, string path) = this.storage.GetTaskhubPathPrefix(this.parameters);
            this.pathPrefix = path;

            var cloudStorageAccount = await this.settings.BlobStorageConnection.GetAzureStorageV11AccountAsync();
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            this.partitionScript = this.cloudBlobContainer.GetBlockBlobReference("partitionscript.json");

            // check that the storage format is supported, and load the relevant FASTER tuning parameters
            BlobManager.LoadAndCheckStorageFormat(this.parameters.StorageFormat, this.settings, this.host.TraceWarning);

            this.connections = new EventHubsConnections(this.settings.EventHubsConnection, EventHubsTransport.PartitionHub, EventHubsTransport.ClientHubs, EventHubsTransport.LoadMonitorHub, this.consumerGroup, this.shutdownSource.Token)
            {
                Host = this.host,
                TraceHelper = this.traceHelper,
            };

            await this.connections.StartAsync(this.parameters);

            if (this.settings.UseSeparateConsumerGroups)
            {
                this.traceHelper.LogInformation($"Determining initial partition offsets for consumer group '{this.consumerGroup}'");
                string formattedFingerprint = this.connections.CreationTimestamp.ToString("o").Replace("/", "-");
                var offsetsBlob = this.cloudBlobContainer.GetBlockBlobReference($"{this.pathPrefix}offsets/{formattedFingerprint}");

                try
                {
                    var jsonText = await offsetsBlob.DownloadTextAsync(this.shutdownSource.Token);
                    this.offsets = JsonConvert.DeserializeObject<Offsets>(jsonText);
                    this.traceHelper.LogInformation($"Loaded initial partition offsets [{string.Join(", ", this.offsets.InitialOffsets)}]");
                }
                catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == (int)System.Net.HttpStatusCode.NotFound)
                {
                    try
                    {
                        this.traceHelper.LogDebug("Creating offsets");
                        this.offsets = new Offsets()
                        {
                            Guid = Guid.NewGuid(),
                            InitialOffsets = await this.connections.GetStartingSequenceNumbers(),
                        };
                        var jsonText = JsonConvert.SerializeObject(this.offsets, Formatting.Indented, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });
                        var noOverwrite = AccessCondition.GenerateIfNoneMatchCondition("*");
                        await offsetsBlob.UploadTextAsync(jsonText, null, noOverwrite, null, null);
                        this.traceHelper.LogInformation($"Created initial partition offsets [{string.Join(", ", this.offsets.InitialOffsets)}]");
                    }
                    catch (StorageException) when (ex.RequestInformation.HttpStatusCode == (int)System.Net.HttpStatusCode.Conflict)
                    {
                        this.traceHelper.LogDebug("Lost creation race, reloading");
                        var jsonText = await offsetsBlob.DownloadTextAsync(this.shutdownSource.Token);
                        this.offsets = JsonConvert.DeserializeObject<Offsets>(jsonText);
                        this.traceHelper.LogInformation($"Loaded initial partition offsets on second attempt [{string.Join(", ", this.offsets.InitialOffsets)}]");
                    }
                }

                this.Fingerprint = $"{this.connections.Fingerprint}-{this.offsets.Guid}";
            }
            else
            {
                this.Fingerprint = this.connections.Fingerprint;
            }

            this.traceHelper.LogInformation($"EventHubs transport connected to partition event hubs with fingerprint {this.Fingerprint}");

            return this.parameters;
        }

        class Offsets
        {
            public Guid Guid { get; set; }

            public List<long> InitialOffsets { get; set; }
        }

        internal long GetInitialOffset(int partitionId)
        {
            if (this.offsets != null)
            {
                return this.offsets.InitialOffsets[partitionId];
            }
            else
            {
                return 0;
            }
        }


        async Task ITransportLayer.StartClientAsync()
        {
            var channel = Channel.CreateBounded<ClientEvent>(new BoundedChannelOptions(500)
            {
                SingleReader = true,
                AllowSynchronousContinuations = true
            });

            PartitionReceiver[] clientReceivers = null;

            int attempt = 0;
            int maxAttempts = 8;

            while (attempt++ < maxAttempts)
            {
                this.ClientId = Guid.NewGuid();
                this.shortClientId = Client.GetShortId(this.ClientId);

                clientReceivers = this.connections.CreateClientReceivers(this.ClientId, this.consumerGroup);

                try
                {
                    this.clientConnectionsEstablished = Enumerable
                        .Range(0, EventHubsConnections.NumClientChannels)
                        .Select(i => this.ClientEstablishConnectionAsync(i, clientReceivers[i]))
                        .ToArray();

                    // we must wait for the client receive connections to be established before continuing
                    // otherwise, we may miss messages that are sent before the client receiver establishes the receive position
                    await Task.WhenAll(this.clientConnectionsEstablished);

                    break; // was successful, so we exit retry loop
                }
                catch (Microsoft.Azure.EventHubs.QuotaExceededException) when (attempt < maxAttempts) 
                {
                    this.traceHelper.LogWarning("EventHubsTransport encountered quota-exceeded exception");
                }
                catch (Exception exception) when (attempt < maxAttempts)
                {
                    this.traceHelper.LogInformation("EventHubsTransport failed with exception {exception}", exception);
                }

                TimeSpan retryDelay = TimeSpan.FromSeconds(1 + attempt * 10);
                this.traceHelper.LogDebug("EventHubsTransport retrying client connection in {retryDelay}", retryDelay);
                Task retryDelayTask = Task.Delay(retryDelay);

                foreach (var clientReceiver in clientReceivers)
                {
                    try
                    {
                        await clientReceiver.CloseAsync();
                    }

                    catch (Exception exception)
                    {
                        this.traceHelper.LogWarning("EventHubsTransport failed to close partition receiver {clientReceiver} during retry: {exception}", clientReceiver, exception);
                    }
                }

                await retryDelayTask;
            }

            this.traceHelper.LogInformation("EventHubsTransport sucessfully established client connection with {fingerPrint} via {consumerGroup}", this.Fingerprint, this.consumerGroup);

            this.clientReceiveLoops = Enumerable
                .Range(0, EventHubsConnections.NumClientChannels)
                .Select(i => this.ClientReceiveLoopAsync(i, clientReceivers[i], channel.Writer))
                .ToArray();

            this.clientProcessTask = this.ClientProcessLoopAsync(channel.Reader);

            this.client = this.host.AddClient(this.ClientId, this.parameters.TaskhubGuid, this);
        }

        async Task ITransportLayer.StartWorkersAsync()
        {
            if (this.client == null)
            {
                throw new InvalidOperationException("client must be started before partition hosting is started.");
            }
            
            if (this.settings.PartitionManagement != PartitionManagementOptions.ClientOnly)
            {
                this.hasWorkers = true;
                await Task.WhenAll(StartPartitionHost(), StartLoadMonitorHost());        
            }

            async Task StartPartitionHost()
            {
                if (this.settings.PartitionManagement != PartitionManagementOptions.Scripted)
                {
                    this.traceHelper.LogInformation($"EventHubsTransport is registering PartitionHost");

                    string formattedCreationDate = this.connections.CreationTimestamp.ToString("o").Replace("/", "-");

                    this.eventProcessorHost = await this.settings.EventHubsConnection.GetEventProcessorHostAsync(
                        Guid.NewGuid().ToString(),
                        EventHubsTransport.PartitionHub,
                        this.consumerGroup,
                        this.settings.BlobStorageConnection,
                        this.cloudBlobContainer.Name,
                        $"{this.pathPrefix}eh-checkpoints/{(EventHubsTransport.PartitionHub)}/{formattedCreationDate}");

                    var processorOptions = new EventProcessorOptions()
                    {
                        MaxBatchSize = 300,
                        PrefetchCount = 500,
                    };

                    await this.eventProcessorHost.RegisterEventProcessorFactoryAsync(
                        new PartitionEventProcessorFactory(this), 
                        processorOptions);

                    this.traceHelper.LogInformation($"EventHubsTransport started PartitionHost");
                }
                else
                {
                    this.traceHelper.LogInformation($"EventHubsTransport is starting scripted partition host");
                    this.scriptedEventProcessorHost = new ScriptedEventProcessorHost(
                            EventHubsTransport.PartitionHub,
                            this.consumerGroup,
                            this.settings.EventHubsConnection,
                            this.settings.BlobStorageConnection,
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
                this.traceHelper.LogInformation("EventHubsTransport is registering LoadMonitorHost");

                this.loadMonitorHost = await this.settings.EventHubsConnection.GetEventProcessorHostAsync(
                        Guid.NewGuid().ToString(),
                        LoadMonitorHub,
                        this.consumerGroup,
                        this.settings.BlobStorageConnection,
                        this.cloudBlobContainer.Name,
                        $"{this.pathPrefix}eh-checkpoints/{LoadMonitorHub}");

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

        internal async Task ExitProcess(bool deletePartitionsFirst)
        {
            if (deletePartitionsFirst)
            {
                await this.connections.DeletePartitions();
            }

            this.traceHelper.LogError("EventHubsTransport is killing process in 10 seconds");
            await Task.Delay(TimeSpan.FromSeconds(10));
            System.Environment.Exit(222);
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

        async Task ITransportLayer.StopAsync(bool fatalExceptionObserved)
        {
            if (Interlocked.CompareExchange(ref this.shutdownTriggered, 1, 0) == 0)
            {
                this.traceHelper.LogInformation("EventHubsTransport is shutting down");
                this.FatalExceptionObserved = fatalExceptionObserved;
                this.shutdownSource.Cancel(); // immediately initiates shutdown of client and of all partitions

                await Task.WhenAll(
                     this.hasWorkers ? this.StopWorkersAsync() : Task.CompletedTask,
                     this.StopClientsAndConnectionsAsync());

                this.traceHelper.LogInformation("EventHubsTransport is shut down");
            }
        }

        async Task StopWorkersAsync()
        {
            this.traceHelper.LogDebug("EventHubsTransport is stopping partition and loadmonitor hosts");
            await Task.WhenAll(
              this.StopPartitionHostAsync(),
              this.StopLoadMonitorHostAsync());
        }

        async Task StopClientsAndConnectionsAsync()
        {
            this.traceHelper.LogDebug("EventHubsTransport is stopping client process loop");
            await this.clientProcessTask;

            this.traceHelper.LogDebug("EventHubsTransport is closing connections");
            await this.connections.StopAsync();

            this.traceHelper.LogDebug("EventHubsTransport is stopping client");
            await this.client.StopAsync();

            this.traceHelper.LogDebug("EventHubsTransport stopped clients");
        }

        async Task StopPartitionHostAsync()
        {
            if (this.settings.PartitionManagement != PartitionManagementOptions.Scripted)
            {
                await this.eventProcessorHost.UnregisterEventProcessorAsync();
            }
            else
            {
                await this.scriptedEventProcessorHost.StopAsync();
            }
            this.traceHelper.LogDebug("EventHubsTransport stopped partition host");
        }

        async Task StopLoadMonitorHostAsync()
        {
            await this.loadMonitorHost.UnregisterEventProcessorAsync();
            this.traceHelper.LogDebug("EventHubsTransport stopped loadmonitor host");
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
                    var clientSender = this.connections.GetClientSender(clientEvent.ClientId, this.settings);
                    clientSender.Submit(clientEvent);
                    break;

                case PartitionEvent partitionEvent:
                    var partitionId = partitionEvent.PartitionId;
                    var partitionSender = this.connections.GetPartitionSender((int) partitionId, this.taskhubGuid, this.settings);
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
                this.traceHelper.LogDebug("Client.{clientId}.ch{index} establishing connection", this.shortClientId, index);
                // receive a dummy packet to establish connection
                // (the packet, if any, cannot be for this receiver because it is fresh)
                await receiver.ReceiveAsync(1, TimeSpan.FromMilliseconds(1));
                this.traceHelper.LogDebug("Client.{clientId}.ch{index} connection established", this.shortClientId, index);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("Client.{clientId}.ch{index} could not connect: {exception}", this.shortClientId, index, exception);
                throw;
            }
        }

        async Task ClientReceiveLoopAsync(int index, PartitionReceiver receiver, ChannelWriter<ClientEvent> channelWriter)
        {
            try
            {
                byte[] clientGuid = this.ClientId.ToByteArray();
                TimeSpan longPollingInterval = TimeSpan.FromMinutes(1);
                var backoffDelay = TimeSpan.Zero;
                string context = $"Client{this.shortClientId}.ch{index}";
                var blobBatchReceiver = new BlobBatchReceiver<ClientEvent>(context, this.traceHelper, this.settings, keepUntilConfirmed: false);

                await this.clientConnectionsEstablished[index];

                while (!this.shutdownSource.IsCancellationRequested)
                {
                    IEnumerable<EventData> packets;

                    try
                    {
                        this.traceHelper.LogTrace("Client{clientId}.ch{index} waiting for new packets", this.shortClientId, index);

                        packets = await receiver.ReceiveAsync(1000, longPollingInterval);

                        backoffDelay = TimeSpan.Zero;
                    }
                    catch (Exception exception) when (!this.shutdownSource.IsCancellationRequested)
                    {
                        if (backoffDelay < TimeSpan.FromSeconds(30))
                        {
                            backoffDelay = backoffDelay + backoffDelay + TimeSpan.FromSeconds(2);
                        }

                        // if we lose access to storage temporarily, we back off, but don't quit
                        this.traceHelper.LogError("Client.{clientId}.ch{index} backing off for {backoffDelay} after error in receive loop: {exception}", this.shortClientId, index, backoffDelay, exception);

                        await Task.Delay(backoffDelay);

                        continue; // retry
                    }

                    if (packets != null)
                    {
                        int totalEvents = 0;
                        var stopwatch = Stopwatch.StartNew();

                        await foreach ((EventData eventData, ClientEvent[] events, long seqNo) in blobBatchReceiver.ReceiveEventsAsync(clientGuid, packets, this.shutdownSource.Token))
                        {
                            for (int i = 0; i < events.Length; i++)
                            {
                                var clientEvent = events[i];

                                clientEvent.ReceiveChannel = index;

                                if (clientEvent.ClientId == this.ClientId)
                                {
                                    this.traceHelper.LogTrace("Client.{clientId}.ch{index} receiving packet #{seqno}.{subSeqNo} {event} id={eventId}", this.shortClientId, index, seqNo, i, clientEvent, clientEvent.EventIdString);
                                    await channelWriter.WriteAsync(clientEvent, this.shutdownSource.Token);
                                    totalEvents++;
                                }
                                else
                                {
                                    this.traceHelper.LogError("Client.{clientId}.ch{index} received packet #{seqno}.{subSeqNo} for client {otherClient}", this.shortClientId, index, seqNo, i, Client.GetShortId(clientEvent.ClientId));
                                }
                            }
                        }
                        this.traceHelper.LogDebug("Client{clientId}.ch{index} received {totalEvents} events in {latencyMs:F2}ms", this.shortClientId, index, totalEvents, stopwatch.Elapsed.TotalMilliseconds);
                    }
                    else
                    {
                        this.traceHelper.LogTrace("Client{clientId}.ch{index} no new packets for last {longPollingInterval}", this.shortClientId, index, longPollingInterval);
                    }
                }
            }
            catch (OperationCanceledException) when (this.shutdownSource.IsCancellationRequested)
            {
                // normal during shutdown
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("Client.{clientId}.ch{index} event processing exception: {exception}", this.shortClientId, index, exception);
            }
            finally
            {
                this.traceHelper.LogInformation("Client.{clientId}.ch{index} exits receive loop", this.shortClientId, index);
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
                this.traceHelper.LogError("Client.{clientId} event processing exception: {exception}", this.shortClientId, exception);
            }
            finally
            {
                this.traceHelper.LogInformation("Client.{clientId} exits process loop", this.shortClientId);
            }
        }
    }
}
