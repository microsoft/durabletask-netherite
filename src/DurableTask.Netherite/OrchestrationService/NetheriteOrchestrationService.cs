﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Faster;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Local partition of the distributed orchestration service.
    /// </summary>
    public class NetheriteOrchestrationService :
        DurableTask.Core.IOrchestrationService,
        DurableTask.Core.IOrchestrationServiceClient,
        DurableTask.Core.IOrchestrationServicePurgeClient,
        DurableTask.Core.Query.IOrchestrationServiceQueryClient,
        DurableTask.Netherite.IOrchestrationServiceQueryClient,
        DurableTask.Core.Entities.IEntityOrchestrationService,
        TransportAbstraction.IHost
    {
        /// <summary>
        /// The type of transport layer that was configured.
        /// </summary>
        public TransportChoices TransportChoice { get; }

        /// <summary>
        /// The type of storage layer that was configured.
        /// </summary>
        public StorageChoices StorageChoice { get; }


        ITransportLayer transport;
        readonly IStorageLayer storage;
        readonly EntityBackendQueriesImplementation EntityBackendQueries;

        readonly WorkItemTraceHelper workItemTraceHelper;

        readonly Stopwatch workItemStopwatch = new Stopwatch();

        /// <summary>
        /// The logger category prefix used for all ILoggers in this backend.
        /// </summary>
        public const string LoggerCategoryName = "DurableTask.Netherite";

        public ITransportLayer TransportLayer => this.transport;
        internal IStorageLayer StorageLayer => this.storage;

        public IServiceProvider ServiceProvider { get; }

        CancellationTokenSource serviceShutdownSource;
        Exception startupException;
        Timer threadWatcher;

        internal async ValueTask<Client> GetClientAsync()
        {
            if (this.checkedClient == null)
            {
                // we need to wait till the startup of the client is complete
                await this.TryStartAsync(true);
            }
            if (this.startupException != null)
            {
                // to help observability we expose backend startup exceptions to client API calls
                throw new InvalidOperationException($"Netherite backend failed to start: {this.startupException.Message}", this.startupException);
            }
            return this.checkedClient;
        }
        Client client;
        Client checkedClient;

        internal NetheriteOrchestrationServiceSettings Settings { get; private set; }
        internal uint NumberPartitions { get; private set; }
        internal string PathPrefix { get; private set; }
        internal string ContainerName { get; private set; }
        internal string StorageAccountName { get; private set; }
        internal TaskhubParameters TaskhubParameters { get; private set; }

        internal WorkItemQueue<ActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkItemQueue<OrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        internal WorkItemQueue<OrchestrationWorkItem> EntityWorkItemQueue { get; private set; }

        internal LoadPublishWorker LoadPublisher { get; private set; }

        internal ILoggerFactory LoggerFactory { get; }
        internal OrchestrationServiceTraceHelper TraceHelper { get; private set; }

        public event Action OnStopping;

        /// <inheritdoc/>
        public override string ToString()
        {
#if DEBUG
            string configuration = "Debug";
#else
            string configuration = "Release";
#endif
            return $"NetheriteOrchestrationService on {this.Settings.TransportChoice}Transport and {this.Settings.StorageChoice}Storage, {configuration} build";
        }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public NetheriteOrchestrationService(NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
            : this(settings, loggerFactory, null)
        {
        }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public NetheriteOrchestrationService(NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory, IServiceProvider serviceProvider)
        {
            this.LoggerFactory = loggerFactory;
            this.ServiceProvider = serviceProvider;
            this.Settings = settings;
            this.TraceHelper = new OrchestrationServiceTraceHelper(loggerFactory, settings.LogLevelLimit, settings.WorkerId, settings.HubName);
            this.workItemTraceHelper = new WorkItemTraceHelper(loggerFactory, settings.WorkItemLogLevelLimit, settings.HubName);
            this.EntityBackendQueries = new EntityBackendQueriesImplementation(this);

            try
            {
                this.TraceHelper.TraceProgress("Reading configuration for transport and storage layers");

                if (!settings.ResolutionComplete)
                {
                    throw new NetheriteConfigurationException("settings must be validated before constructing orchestration service");
                }

                // determine a storage account name to be used for tracing
                this.StorageAccountName = this.Settings.StorageAccountName;

                this.TraceHelper.TraceCreated(Environment.ProcessorCount, this.Settings.TransportChoice, this.Settings.StorageChoice);

                // construct the storage layer
                switch (this.Settings.StorageChoice)
                {
                    case StorageChoices.Memory:
                        this.storage = new MemoryStorageLayer(this.Settings, this.TraceHelper.Logger);
                        break;

                    case StorageChoices.Faster:
                        this.storage = new FasterStorageLayer(this.Settings, this.TraceHelper, this.LoggerFactory);
                        break;

                    case StorageChoices.Custom:
                        var storageLayerFactory = this.ServiceProvider?.GetService<IStorageLayerFactory>();
                        if (storageLayerFactory == null)
                        {
                            throw new NetheriteConfigurationException("could not find injected IStorageLayerFactory");
                        }
                        this.storage = storageLayerFactory.Create(this);
                        break;
                }

                this.ConstructTransportLayer();

                this.workItemStopwatch.Start();

                this.TraceHelper.TraceProgress(
                    $"Configured trace generation limits: general={settings.LogLevelLimit} , transport={settings.TransportLogLevelLimit}, storage={settings.StorageLogLevelLimit}, "
                    + $"events={settings.EventLogLevelLimit}; workitems={settings.WorkItemLogLevelLimit};  clients={settings.ClientLogLevelLimit}; loadmonitor={settings.LoadMonitorLogLevelLimit}; etwEnabled={EtwSource.Log.IsEnabled()}; "
                    + $"core.IsTraceEnabled={DurableTask.Core.Tracing.DefaultEventSource.Log.IsTraceEnabled}");

                if (this.Settings.TestHooks != null)
                {
                    this.Settings.TestHooks.OnError += (string message) => this.TraceHelper.TraceError("TestHook error", message);
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.TraceError("Could not create NetheriteOrchestrationService", e);
                throw;
            }
        }

        void ConstructTransportLayer()
        {
            // construct the transport layer
            switch (this.Settings.TransportChoice)
            {
                case TransportChoices.SingleHost:
                    this.transport = new SingleHostTransport.SingleHostTransportLayer(this, this.Settings, this.storage, this.TraceHelper.Logger);
                    break;

                case TransportChoices.EventHubs:
                    this.transport = new EventHubsTransport.EventHubsTransport(this, this.Settings, this.storage, this.LoggerFactory);
                    break;

                case TransportChoices.Custom:
                    var transportLayerFactory = this.ServiceProvider?.GetService<ITransportLayerFactory>();
                    if (transportLayerFactory == null)
                    {
                        throw new NetheriteConfigurationException("could not find injected ITransportLayerFactory");
                    }
                    this.transport = transportLayerFactory.Create(this);
                    break;
            }
        }


        /// <summary>
        /// Get a scaling monitor for autoscaling.
        /// </summary>
        /// <param name="monitor">The returned scaling monitor.</param>
        /// <returns>true if autoscaling is supported, false otherwise</returns>
        public bool TryGetScalingMonitor(out ScalingMonitor monitor)
        {
            if (this.Settings.StorageChoice == StorageChoices.Faster
                && this.Settings.TransportChoice == TransportChoices.EventHubs)
            {
                try
                {
                    ILoadPublisherService loadPublisher = string.IsNullOrEmpty(this.Settings.LoadInformationAzureTableName) ?
                        new AzureBlobLoadPublisher(this.Settings.BlobStorageConnection, this.Settings.HubName, this.Settings.TaskhubParametersFilePath)
                        : new AzureTableLoadPublisher(this.Settings.TableStorageConnection, this.Settings.LoadInformationAzureTableName, this.Settings.HubName);

                    NetheriteMetricsProvider netheriteMetricsProvider = this.GetNetheriteMetricsProvider(loadPublisher, this.Settings.EventHubsConnection);

                    monitor = new ScalingMonitor(
                        loadPublisher,
                        this.Settings.EventHubsConnection,
                        this.Settings.LoadInformationAzureTableName,
                        this.Settings.HubName,
                        this.TraceHelper.TraceScaleRecommendation,
                        this.TraceHelper.TraceProgress,
                        this.TraceHelper.TraceError,
                        netheriteMetricsProvider);

                    return true;
                }
                catch (Exception e)
                {
                    this.TraceHelper.TraceError("ScaleMonitor failure during construction", e);
                }
            }

            monitor = null;
            return false;
        }

        internal ILoadPublisherService GetLoadPublisher()
        {
            return string.IsNullOrEmpty(this.Settings.LoadInformationAzureTableName) ?
                new AzureBlobLoadPublisher(this.Settings.BlobStorageConnection, this.Settings.HubName, this.Settings.TaskhubParametersFilePath)
                : new AzureTableLoadPublisher(this.Settings.TableStorageConnection, this.Settings.LoadInformationAzureTableName, this.Settings.HubName);
        }

        internal NetheriteMetricsProvider GetNetheriteMetricsProvider(ILoadPublisherService loadPublisher, ConnectionInfo eventHubsConnection)
        {
            return new NetheriteMetricsProvider(loadPublisher, eventHubsConnection);
        }

        public void WatchThreads(object _)
        {
            if (TrackedThreads.NumberThreads > 100)
            {
                this.TraceHelper.TraceError("Too many threads, shutting down", TrackedThreads.GetThreadNames());
                Thread.Sleep(TimeSpan.FromSeconds(60));
                System.Environment.Exit(333);
            }
        }


        /******************************/
        // management methods
        /******************************/

        /// <inheritdoc />
        async Task IOrchestrationService.CreateAsync() => await ((IOrchestrationService)this).CreateAsync(true);

        /// <inheritdoc />
        async Task IOrchestrationService.CreateAsync(bool recreateInstanceStore)
        {
            if ((await this.storage.TryLoadTaskhubAsync(throwIfNotFound: false)) != null)
            {
                if (recreateInstanceStore)
                {
                    this.TraceHelper.TraceProgress("Creating");

                    await this.storage.DeleteTaskhubAsync();
                    await this.storage.CreateTaskhubIfNotExistsAsync();
                }
            }
            else
            {
                await this.storage.CreateTaskhubIfNotExistsAsync();
            }
        }

        /// <inheritdoc />
        async Task IOrchestrationService.CreateIfNotExistsAsync() => await ((IOrchestrationService)this).CreateAsync(false);

        /// <inheritdoc />
        async Task IOrchestrationService.DeleteAsync()
        {
            await this.storage.DeleteTaskhubAsync();
        }

        /// <inheritdoc />
        async Task IOrchestrationService.DeleteAsync(bool deleteInstanceStore) => await ((IOrchestrationService)this).DeleteAsync();

        /// <inheritdoc />
        Task IOrchestrationService.StartAsync()
        {
            // Some hosts (like Azure Functions) may retry the StartAsync operation multiple times.
            // See: https://github.com/microsoft/durabletask-netherite/issues/352//
            // therefore, we first check if this is a retry, i.e. startup has already failed,
            // in which case we reset the state first.

            var lastTransition = this.currentTransition;
            if (lastTransition.IsFaulted) 
            {
                // We use the TryTransition helper to ensure that only one thread resets the state, if there are races.
                TryTransition(ref this.currentTransition, lastTransition, this.ResetAsync);
            }

            return this.TryStartAsync(false);
        }

        /// <inheritdoc />
        Task IOrchestrationService.StopAsync(bool quickly)
        {
            return this.TryStopAsync(quickly);
        }

        /// <inheritdoc />
        Task IOrchestrationService.StopAsync() => this.TryStopAsync(false);

        enum ServiceState
        {
            None, Client, Full
        }

        Task<ServiceState> currentTransition = Task.FromResult(ServiceState.None);

        public async Task TryStartAsync(bool clientOnly)
        {
            clientOnly = clientOnly || this.Settings.PartitionManagement == PartitionManagementOptions.ClientOnly;

            while (true)
            {
                var lastTransition = this.currentTransition;
                var currentState = await lastTransition;

                if (currentState == ServiceState.None)
                {
                    TryTransition(ref this.currentTransition, lastTransition, this.StartClientAsync);

                    continue;
                }

                if (currentState == ServiceState.Client)
                {
                    if (clientOnly)
                    {
                        return;
                    }

                    TryTransition(ref this.currentTransition, lastTransition, this.StartWorkersAsync);

                    continue;
                }

                return;
            }
        }

        static void TryTransition<T>(ref Task<T> currentTransition, Task<T> lastTransition, Func<Task<T>> nextTransition)
        {
            // some other thread could be trying to do a transition at the same time, creating a race.
            var waitForRaceToBeDetermined = new TaskCompletionSource<bool>();
            // wrap the transition in a async function that waits for the race to be won before executing the transition
            var task = conditionalTransition();

            // to resolve any potential races, we use an interlocked operation. This operation replaces the currentTransition
            // ONLY if the lastTransition matches the lastTransition that was observed earlier. Otherwise it does nothing.
            var interlockedResult = Interlocked.CompareExchange<Task<T>>(ref currentTransition, task, lastTransition);
            bool interlockedWasEffective = interlockedResult == lastTransition; // interlocked exchange returns the value it read
            waitForRaceToBeDetermined.SetResult(interlockedWasEffective);

            async Task<T> conditionalTransition()
            {
                if (await waitForRaceToBeDetermined.Task)
                {
                    // this thread won the race so we are now executing the transition.
                    return await nextTransition();
                }
                else
                {
                    // this thread lost the race so the task does nothing.
                    return default; 
                }
            }
        }

        async Task<ServiceState> StartClientAsync()
        {
            try
            {
                this.TraceHelper.TraceProgress("Starting Client");

                this.serviceShutdownSource ??= new CancellationTokenSource();

                if (this.Settings.TestHooks != null)
                {
                    this.TraceHelper.TraceProgress(this.Settings.TestHooks.ToString());

                    if (this.Settings.TestHooks.FaultInjectionActive)
                    {
                        this.Settings.TestHooks.FaultInjector.ClientStartup();
                    }
                }

                this.TaskhubParameters = await this.transport.StartAsync();
                (this.ContainerName, this.PathPrefix) = this.storage.GetTaskhubPathPrefix(this.TaskhubParameters);
                this.NumberPartitions = (uint) this.TaskhubParameters.PartitionCount;

                if (this.Settings.PartitionCount != this.NumberPartitions)
                {
                    this.TraceHelper.TraceWarning($"Ignoring configuration setting partitionCount={this.Settings.PartitionCount} because existing TaskHub has {this.NumberPartitions} partitions");
                }

                await this.transport.StartClientAsync();

                System.Diagnostics.Debug.Assert(this.client != null, "transport layer should have added client");

                this.checkedClient = this.client;

                this.ActivityWorkItemQueue = new WorkItemQueue<ActivityWorkItem>();
                this.OrchestrationWorkItemQueue = new WorkItemQueue<OrchestrationWorkItem>();
                this.EntityWorkItemQueue = new WorkItemQueue<OrchestrationWorkItem>();

                this.TraceHelper.TraceProgress($"Started client");

                return ServiceState.Client;
            }
            catch (Exception e)
            {
                this.startupException = e;

                this.TraceHelper.TraceError($"Failed to start: {e.Message}", e);

                // invoke cancellation so that any partially-started partitions and event loops are terminated
                try
                {
                    this.serviceShutdownSource.Cancel();
                    this.serviceShutdownSource.Dispose();
                    this.serviceShutdownSource = null;
                }
                catch (Exception shutdownException)
                {
                    this.TraceHelper.TraceError($"Exception while shutting down service: {shutdownException.Message}", shutdownException);
                }

                throw;
            }
        }

        async Task<ServiceState> StartWorkersAsync()
        {
            try
            {
                System.Diagnostics.Debug.Assert(this.client != null, "transport layer should have added client");

                this.TraceHelper.TraceProgress("Starting Workers");

                LeaseTimer.Instance.DelayWarning = (int delay) =>
                    this.TraceHelper.TraceWarning($"Lease timer is running {delay}s behind schedule");

                if (this.storage.LoadPublisher != null)
                {
                    this.TraceHelper.TraceProgress("Starting Load Publisher");
                    this.LoadPublisher = new LoadPublishWorker(this.storage.LoadPublisher, this.TraceHelper);
                }

                await this.transport.StartWorkersAsync();

                if (this.threadWatcher == null)
                {
                    this.threadWatcher = new Timer(this.WatchThreads, null, 0, 120000);
                }

                this.TraceHelper.TraceProgress($"Started partitionCount={this.NumberPartitions}");

                return ServiceState.Full;
            }
            catch (Exception e)
            {
                this.startupException = e;

                this.TraceHelper.TraceError($"Failed to start: {e.Message}", e);

                // invoke cancellation so that any partially-started partitions and event loops are terminated
                try
                {
                    this.serviceShutdownSource.Cancel();
                    this.serviceShutdownSource.Dispose();
                    this.serviceShutdownSource = null;
                }
                catch(Exception shutdownException)
                {
                    this.TraceHelper.TraceError($"Exception while shutting down service: {shutdownException.Message}", shutdownException);
                }

                throw;
            }
        }

        Task<ServiceState> ResetAsync()
        {
            this.TraceHelper.TraceProgress("Resetting Orchestration Service");

            this.client = null;
            this.checkedClient = null;
            this.startupException = null;
            this.ConstructTransportLayer(); // we need to recreate the transport layer because it was not designed to be retried

            return Task.FromResult(ServiceState.None);
        }

        async Task<ServiceState> TryStopAsync(bool quickly)
        {
            try
            {
                this.TraceHelper.TraceProgress($"Stopping quickly={quickly}");

                this.OnStopping?.Invoke();

                if (this.serviceShutdownSource != null)
                {
                    this.serviceShutdownSource.Cancel();
                    this.serviceShutdownSource.Dispose();
                    this.serviceShutdownSource = null;

                    await this.transport.StopAsync(fatalExceptionObserved: false);

                    this.ActivityWorkItemQueue.Dispose();
                    this.EntityWorkItemQueue.Dispose();
                    this.OrchestrationWorkItemQueue.Dispose();
                }

                this.threadWatcher?.Dispose();
                this.threadWatcher = null;

                this.TraceHelper.TraceProgress("Stopped cleanly");

                return ServiceState.None;
            }
            catch (Exception e)
            {
                this.TraceHelper.TraceError($"Failed to stop cleanly: {e.Message}", e);
                throw;
            }
            finally
            {
                this.TraceHelper.TraceStopped();
            }
        }

        /// <summary>
        /// Computes the partition for the given instance.
        /// </summary>
        /// <param name="instanceId">The instance id.</param>
        /// <returns>The partition id.</returns>
        public uint GetPartitionId(string instanceId)
        {
            int placementSeparatorPosition = instanceId.LastIndexOf('!');

            // if the instance id ends with !nn, where nn is a two-digit number, it indicates explicit partition placement
            if (placementSeparatorPosition != -1
                && placementSeparatorPosition <= instanceId.Length - 2
                && uint.TryParse(instanceId.Substring(placementSeparatorPosition + 1), out uint index))
            {
                var partitionId = index % this.NumberPartitions;
                //this.Logger.LogTrace($"Instance: {instanceId} was explicitly placed on partition: {partitionId}");
                return partitionId;
            }
            else
            {
                return Fnv1aHashHelper.ComputeHash(instanceId) % this.NumberPartitions;
            }
        }

        uint GetNumberPartitions() => this.NumberPartitions;

        /******************************/
        // host methods
        /******************************/

        IStorageLayer TransportAbstraction.IHost.StorageLayer => this.storage;

        TransportAbstraction.IClient TransportAbstraction.IHost.AddClient(Guid clientId, Guid taskHubGuid, TransportAbstraction.ISender batchSender)
        {
            System.Diagnostics.Debug.Assert(this.client == null, "Backend should create only 1 client");

            this.client = new Client(this, clientId, taskHubGuid, batchSender, this.workItemTraceHelper, this.serviceShutdownSource.Token);
            return this.client;
        }

        TransportAbstraction.IPartition TransportAbstraction.IHost.AddPartition(uint partitionId, TransportAbstraction.ISender batchSender)
        {
            var partition = new Partition(this, partitionId, this.GetPartitionId, this.GetNumberPartitions, batchSender, this.Settings, this.StorageAccountName,
                this.ActivityWorkItemQueue, this.OrchestrationWorkItemQueue, this.EntityWorkItemQueue, this.LoadPublisher, this.workItemTraceHelper);

            return partition;
        }

        TransportAbstraction.ILoadMonitor TransportAbstraction.IHost.AddLoadMonitor(Guid taskHubGuid, TransportAbstraction.ISender batchSender)
        {
            return new LoadMonitor(this, taskHubGuid, batchSender);
        }

        IPartitionErrorHandler TransportAbstraction.IHost.CreateErrorHandler(uint partitionId)
        {
            return new PartitionErrorHandler((int)partitionId, this.TraceHelper.Logger, this.Settings.LogLevelLimit, this.StorageAccountName, this.Settings.HubName, this);
        }

        void TransportAbstraction.IHost.TraceWarning(string message)
        {
            this.TraceHelper.TraceWarning(message);
        }

        void TransportAbstraction.IHost.OnFatalExceptionObserved(Exception e)
        {
            if (this.Settings.EmergencyShutdownOnFatalExceptions)
            {
                Task.Run(async() =>
                {
                    this.TraceHelper.TraceError($"OrchestrationService is initiating an emergency shutdown due to a fatal {e.GetType().FullName}", e);

                    // try to stop the transport as quickly as possible, and don't wait longer than 30 seconds
                    await Task.WhenAny(this.transport.StopAsync(fatalExceptionObserved: true), Task.Delay(TimeSpan.FromSeconds(30)));

                    this.TraceHelper.TraceWarning($"OrchestrationService is killing process in 10 seconds");
                    await Task.Delay(TimeSpan.FromSeconds(10));

                    System.Environment.Exit(333);
                });
            }
        }

        /******************************/
        // client methods
        /******************************/

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage)
            => await (await this.GetClientAsync().ConfigureAwait(false)).CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                null).ConfigureAwait(false);

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
            => await (await this.GetClientAsync().ConfigureAwait(false)).CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                dedupeStatuses).ConfigureAwait(false);

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.SendTaskOrchestrationMessageAsync(TaskMessage message)
            => await (await this.GetClientAsync().ConfigureAwait(false)).SendTaskOrchestrationMessageBatchAsync(
                this.GetPartitionId(message.OrchestrationInstance.InstanceId),
                new[] { message }).ConfigureAwait(false);

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            var client = await this.GetClientAsync().ConfigureAwait(false);
            if (messages.Length != 0)
            {
                await Task.WhenAll(messages
                    .GroupBy(tm => this.GetPartitionId(tm.OrchestrationInstance.InstanceId))
                    .Select(group => client.SendTaskOrchestrationMessageBatchAsync(group.Key, group))
                    .ToList())
                    .ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        async Task<OrchestrationState> IOrchestrationServiceClient.WaitForOrchestrationAsync(
                string instanceId,
                string executionId,
                TimeSpan timeout,
                CancellationToken cancellationToken)
        {
            Client client = await this.GetClientAsync().ConfigureAwait(false);
            DateTime deadlineUtc = (timeout.TotalMilliseconds == -1) ? DateTime.MaxValue.ToUniversalTime() : DateTime.UtcNow + timeout;

            while (true)
            {
                var nextTimeout = deadlineUtc - DateTime.UtcNow;

                if (nextTimeout <= TimeSpan.Zero)
                {
                    // we are out of time, and return null to indicate that the request timed out.
                    // Returning null is consistent with the behavior of WaitForOrchestrationAsync in other backends.
                    return null;
                }
                else if (nextTimeout > TimeSpan.FromMinutes(5))
                {
                    // we do not want wait requests to remain in the system for a very long time
                    // (because it could potentially cause issues with partition state size)
                    // so we break long timeouts into 5 minute chunks
                    nextTimeout = TimeSpan.FromMinutes(5);
                }

                OrchestrationState response = await client.WaitForOrchestrationAsync(
                    this.GetPartitionId(instanceId),
                    instanceId,
                    executionId,
                    nextTimeout,
                    cancellationToken).ConfigureAwait(false);

                if (response != null)
                {
                    return response;
                }
            }
        }

        /// <inheritdoc />
        async Task<OrchestrationState> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId,
            string executionId)
        {
            var state = await (await this.GetClientAsync().ConfigureAwait(false)).GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, true).ConfigureAwait(false);
            return state != null && (executionId == null || executionId == state.OrchestrationInstance.ExecutionId)
                ? state
                : null;
        }

        /// <inheritdoc />
        async Task<IList<OrchestrationState>> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId,
            bool allExecutions)
        {
            // note: allExecutions is always ignored because storage contains never more than one execution.
            var state = await (await this.GetClientAsync().ConfigureAwait(false)).GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, true).ConfigureAwait(false);
            return state != null
                ? (new[] { state })
                : (new OrchestrationState[0]);
        }

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(
                string instanceId,
                string message)
            => await (await this.GetClientAsync().ConfigureAwait(false)).ForceTerminateTaskOrchestrationAsync(this.GetPartitionId(instanceId), instanceId, message).ConfigureAwait(false);

        /// <inheritdoc />
        async Task<string> IOrchestrationServiceClient.GetOrchestrationHistoryAsync(
            string instanceId,
            string executionId)
        {
            var client = await this.GetClientAsync().ConfigureAwait(false);
            (string actualExecutionId, IList<HistoryEvent> history) =
                await client.GetOrchestrationHistoryAsync(this.GetPartitionId(instanceId), instanceId).ConfigureAwait(false);

            if (history != null && (executionId == null || executionId == actualExecutionId))
            {
                return JsonConvert.SerializeObject(history);
            }
            else
            {
                return JsonConvert.SerializeObject(new List<HistoryEvent>());
            }
        }

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType
            timeRangeFilterType)
        {
            if (timeRangeFilterType != OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter)
            {
                throw new NotSupportedException("Purging is supported only for Orchestration created time filter.");
            }

            await (await this.GetClientAsync().ConfigureAwait(false)).PurgeInstanceHistoryAsync(thresholdDateTimeUtc, null, null).ConfigureAwait(false);
        }

        /// <inheritdoc />
        async Task<OrchestrationState> IOrchestrationServiceQueryClient.GetOrchestrationStateAsync(string instanceId, bool fetchInput, bool fetchOutput)
        {
            return await (await this.GetClientAsync().ConfigureAwait(false)).GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, fetchInput, fetchOutput).ConfigureAwait(false);
        }

        /// <inheritdoc />
        async Task<IList<OrchestrationState>> IOrchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken cancellationToken)
            => await (await this.GetClientAsync().ConfigureAwait(false)).GetOrchestrationStateAsync(cancellationToken).ConfigureAwait(false);

        /// <inheritdoc />
        async Task<IList<OrchestrationState>> IOrchestrationServiceQueryClient.GetOrchestrationStateAsync(DateTime? CreatedTimeFrom, DateTime? CreatedTimeTo, IEnumerable<OrchestrationStatus> RuntimeStatus, string InstanceIdPrefix, CancellationToken CancellationToken)
            => await (await this.GetClientAsync().ConfigureAwait(false)).GetOrchestrationStateAsync(CreatedTimeFrom, CreatedTimeTo, RuntimeStatus, InstanceIdPrefix, CancellationToken).ConfigureAwait(false);

        /// <inheritdoc />
        async Task<int> IOrchestrationServiceQueryClient.PurgeInstanceHistoryAsync(string instanceId)
            => await (await this.GetClientAsync().ConfigureAwait(false)).DeleteAllDataForOrchestrationInstance(this.GetPartitionId(instanceId), instanceId).ConfigureAwait(false);

        /// <inheritdoc />
        async Task<int> IOrchestrationServiceQueryClient.PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
            => await (await this.GetClientAsync().ConfigureAwait(false)).PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus).ConfigureAwait(false);

        /// <inheritdoc />
        async Task<InstanceQueryResult> IOrchestrationServiceQueryClient.QueryOrchestrationStatesAsync(InstanceQuery instanceQuery, int pageSize, string continuationToken, CancellationToken cancellationToken)
        {
            return await (await this.GetClientAsync().ConfigureAwait(false)).QueryOrchestrationStatesAsync(instanceQuery, pageSize, continuationToken, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        async Task<DurableTask.Core.Query.OrchestrationQueryResult> DurableTask.Core.Query.IOrchestrationServiceQueryClient.GetOrchestrationWithQueryAsync(
            DurableTask.Core.Query.OrchestrationQuery query,
            CancellationToken cancellationToken)
        {
            InstanceQuery instanceQuery = new()
            {
                CreatedTimeFrom = query.CreatedTimeFrom,
                CreatedTimeTo = query.CreatedTimeTo,
                ExcludeEntities = query.ExcludeEntities,
                FetchInput = query.FetchInputsAndOutputs,
                InstanceIdPrefix = query.InstanceIdPrefix,
                PrefetchHistory = false,
                RuntimeStatus = query.RuntimeStatus?.ToArray(),
            };

            Client client = await this.GetClientAsync().ConfigureAwait(false);
            InstanceQueryResult result = await client.QueryOrchestrationStatesAsync(instanceQuery, query.PageSize, query.ContinuationToken, cancellationToken).ConfigureAwait(false);
            return new DurableTask.Core.Query.OrchestrationQueryResult(result.Instances.ToList(), result.ContinuationToken);
        }

        /// <inheritdoc />
        async Task<PurgeResult> IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(string instanceId)
            => new PurgeResult(await (await this.GetClientAsync().ConfigureAwait(false)).DeleteAllDataForOrchestrationInstance(this.GetPartitionId(instanceId), instanceId).ConfigureAwait(false));

        /// <inheritdoc />
        async Task<PurgeResult> IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
            => new PurgeResult(await (await this.GetClientAsync()).PurgeInstanceHistoryAsync(purgeInstanceFilter.CreatedTimeFrom, purgeInstanceFilter.CreatedTimeTo, purgeInstanceFilter.RuntimeStatus));

        /// <inheritdoc />
        EntityBackendQueries IEntityOrchestrationService.EntityBackendQueries => this.EntityBackendQueries;

        class EntityBackendQueriesImplementation : EntityBackendQueries
        {
            readonly NetheriteOrchestrationService service;

            public EntityBackendQueriesImplementation(NetheriteOrchestrationService netheriteOrchestrationService)
            {
                this.service = netheriteOrchestrationService;
            }
            public override async Task<EntityMetadata?> GetEntityAsync(EntityId id, bool includeState = false, bool includeTransient = false, CancellationToken cancellation = default)
            {
                string instanceId = id.ToString();
                OrchestrationState state = await(await this.service.GetClientAsync().ConfigureAwait(false))
                    .GetOrchestrationStateAsync(this.service.GetPartitionId(instanceId.ToString()), instanceId, fetchInput: includeState, false).ConfigureAwait(false);

                return this.GetEntityMetadata(state, includeState, includeTransient);
            }

            public override async Task<EntityQueryResult> QueryEntitiesAsync(EntityQuery filter, CancellationToken cancellation)
            {
                string adjustedPrefix = string.IsNullOrEmpty(filter.InstanceIdStartsWith) ? "@" : filter.InstanceIdStartsWith;

                if (adjustedPrefix[0] != '@')
                {
                    return new EntityQueryResult()
                    {
                        Results = new List<EntityMetadata>(),
                        ContinuationToken = null,
                    };
                }

                var condition = new InstanceQuery()
                {
                    InstanceIdPrefix = adjustedPrefix,
                    CreatedTimeFrom = filter.LastModifiedFrom,
                    CreatedTimeTo = filter.LastModifiedTo,
                    FetchInput = filter.IncludeState,
                    PrefetchHistory = false,
                    ExcludeEntities = false,
                }; 

                List<EntityMetadata> metadataList = new List<EntityMetadata>();

                InstanceQueryResult result = await (await this.service.GetClientAsync().ConfigureAwait(false))
                    .QueryOrchestrationStatesAsync(condition, filter.PageSize ?? 200, filter.ContinuationToken, cancellation).ConfigureAwait(false);

                foreach(var entry in result.Instances)
                {
                    var metadata = this.GetEntityMetadata(entry, filter.IncludeState, filter.IncludeTransient);
                    if (metadata.HasValue)
                    {
                        metadataList.Add(metadata.Value);
                    }
                }

                return new EntityQueryResult()
                {
                    Results = metadataList,
                    ContinuationToken = result.ContinuationToken,
                };
            }

            public override async Task<CleanEntityStorageResult> CleanEntityStorageAsync(CleanEntityStorageRequest request = default, CancellationToken cancellation = default)
            {
                if (!request.ReleaseOrphanedLocks)
                {
                    // there is no need to do anything since deletion is implicit
                    return new CleanEntityStorageResult();
                }

                var condition = new InstanceQuery()
                {
                    InstanceIdPrefix = "@",
                    FetchInput = false,
                    PrefetchHistory = false,
                    ExcludeEntities = false,
                };

                var client = await this.service.GetClientAsync().ConfigureAwait(false);

                string continuationToken = null;
                int orphanedLocksReleased = 0;
                            
                // list all entities (without fetching the input) and for each locked one,
                // check if the lock owner is still running. If not, release the lock.
                do
                {
                    var page = await client.QueryOrchestrationStatesAsync(condition, 500, continuationToken, cancellation).ConfigureAwait(false);

                    // The checks run in parallel for all entities in the page
                    List<Task> tasks = new List<Task>();
                    foreach (var state in page.Instances)
                    {
                        EntityStatus status = ClientEntityHelpers.GetEntityStatus(state.Status);
                        if (status != null && status.LockedBy != null)
                        {
                            tasks.Add(CheckForOrphanedLockAndFixIt(state, status.LockedBy));
                        }
                    }

                    async Task CheckForOrphanedLockAndFixIt(OrchestrationState state, string lockOwner)
                    {
                        uint partitionId = this.service.GetPartitionId(lockOwner);

                        OrchestrationState ownerState
                            = await client.GetOrchestrationStateAsync(partitionId, lockOwner, fetchInput: false, fetchOutput: false);

                        bool OrchestrationIsRunning(OrchestrationStatus? status)
                            => status != null && (status == OrchestrationStatus.Running || status == OrchestrationStatus.Suspended);

                        if (!OrchestrationIsRunning(ownerState?.OrchestrationStatus))
                        {
                            // the owner is not a running orchestration. Send a lock release.
                            EntityMessageEvent eventToSend = ClientEntityHelpers.EmitUnlockForOrphanedLock(state.OrchestrationInstance, lockOwner);
                            await client.SendTaskOrchestrationMessageBatchAsync(
                                this.service.GetPartitionId(state.OrchestrationInstance.InstanceId),
                                new TaskMessage[] { eventToSend.AsTaskMessage() });

                            Interlocked.Increment(ref orphanedLocksReleased);
                        }
                    }

                    // wait for all of the checks to finish before moving on to the next page.
                    await Task.WhenAll(tasks);
                }
                while (continuationToken != null);

                return new CleanEntityStorageResult()
                {
                    EmptyEntitiesRemoved = 0,
                    OrphanedLocksReleased = orphanedLocksReleased,
                };
            }

            EntityMetadata? GetEntityMetadata(OrchestrationState state, bool includeState, bool includeTransient)
            {
                if (state != null)
                {
                    // determine the status of the entity by deserializing the custom status field
                    EntityStatus status = ClientEntityHelpers.GetEntityStatus(state.Status);

                    if (status?.EntityExists == true || includeTransient)
                    {
                        return new EntityMetadata()
                        {
                            EntityId = EntityId.FromString(state.OrchestrationInstance.InstanceId),
                            LastModifiedTime = state.CreatedTime,
                            SerializedState = (includeState && status?.EntityExists == true) ? ClientEntityHelpers.GetEntityState(state.Input) : null,
                            LockedBy = status?.LockedBy,
                            BacklogQueueSize = status?.BacklogQueueSize ?? 0,
                        };
                    }
                }

                return null;               
            }
        }


        /******************************/
        // Task orchestration methods
        /******************************/

        Task<TaskOrchestrationWorkItem> IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        => this.LockNextWorkItemInternal(this.OrchestrationWorkItemQueue, receiveTimeout, cancellationToken);

        Task<TaskOrchestrationWorkItem> IEntityOrchestrationService.LockNextOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        => this.LockNextWorkItemInternal(this.OrchestrationWorkItemQueue, receiveTimeout, cancellationToken);

        Task<TaskOrchestrationWorkItem> IEntityOrchestrationService.LockNextEntityWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        => this.LockNextWorkItemInternal(this.EntityWorkItemQueue, receiveTimeout, cancellationToken);

        async Task<TaskOrchestrationWorkItem> LockNextWorkItemInternal(WorkItemQueue<OrchestrationWorkItem> workItemQueue, TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            var nextOrchestrationWorkItem = await workItemQueue.GetNext(receiveTimeout, cancellationToken).ConfigureAwait(false);

            if (nextOrchestrationWorkItem != null)
            {
                nextOrchestrationWorkItem.MessageBatch.WaitingSince = null;

                this.workItemTraceHelper.TraceWorkItemStarted(
                    nextOrchestrationWorkItem.Partition.PartitionId,
                    nextOrchestrationWorkItem.WorkItemType,
                    nextOrchestrationWorkItem.MessageBatch.WorkItemId,
                    nextOrchestrationWorkItem.MessageBatch.InstanceId,
                    nextOrchestrationWorkItem.Type.ToString(),
                    WorkItemTraceHelper.FormatMessageIdList(nextOrchestrationWorkItem.MessageBatch.TracedMessages));

                nextOrchestrationWorkItem.StartedAt = this.workItemStopwatch.Elapsed.TotalMilliseconds;
            }

            return nextOrchestrationWorkItem;
        }

        Task IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> activityMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;
            var messageBatch = orchestrationWorkItem.MessageBatch;
            var partition = orchestrationWorkItem.Partition;
            var latencyMs = this.workItemStopwatch.Elapsed.TotalMilliseconds - orchestrationWorkItem.StartedAt;

            List<TaskMessage> localMessages = null;
            List<TaskMessage> remoteMessages = null;

            // all continue as new requests are processed immediately (DurableTask.Core always uses "fast" continue-as-new)
            // so by the time we get here, it is not a continue as new
            partition.Assert(continuedAsNewMessage == null, "unexpected continueAsNew message");
            partition.Assert(workItem.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew, "unexpected continueAsNew status");

            // we assign sequence numbers to all outgoing messages, to help us track them using unique message ids
            long sequenceNumber = 0;

            if (activityMessages != null)
            {
                foreach(TaskMessage taskMessage in activityMessages)
                {
                    taskMessage.SequenceNumber = sequenceNumber++;
                }
            }

            if (orchestratorMessages != null)
            {
                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    taskMessage.SequenceNumber = sequenceNumber++;
                    if (partition.PartitionId == partition.PartitionFunction(taskMessage.OrchestrationInstance.InstanceId))
                    {
                        if (Entities.IsDelayedEntityMessage(taskMessage, out _))
                        {
                            (timerMessages ??= new List<TaskMessage>()).Add(taskMessage);
                        }
                        else if (taskMessage.Event is ExecutionStartedEvent executionStartedEvent && executionStartedEvent.ScheduledStartTime.HasValue)
                        {
                            (timerMessages ??= new List<TaskMessage>()).Add(taskMessage);
                        }
                        else
                        {
                            (localMessages ??= new List<TaskMessage>()).Add(taskMessage);
                        }
                    }
                    else
                    {
                        (remoteMessages ??= new List<TaskMessage>()).Add(taskMessage);
                    }
                }
            }

            if (timerMessages != null)
            {
                foreach (TaskMessage taskMessage in timerMessages)
                {
                    taskMessage.SequenceNumber = sequenceNumber++;
                }
            }

            if (partition.ErrorHandler.IsTerminated)
            {
                // we get here if the partition was terminated. The work is thrown away. 
                // It's unavoidable by design, but let's at least create a warning.
                this.workItemTraceHelper.TraceWorkItemDiscarded(
                    partition.PartitionId,
                    orchestrationWorkItem.WorkItemType,
                    messageBatch.WorkItemId,
                    workItem.InstanceId,
                    "",
                    "partition was terminated");

                return Task.CompletedTask;
            }

            // if this orchestration is not done, and extended sessions are enabled, we keep the work item so we can reuse the execution cursor
            bool cacheWorkItemForReuse = partition.Settings.CacheOrchestrationCursors && state.OrchestrationStatus == OrchestrationStatus.Running;

            BatchProcessed batchProcessedEvent = new BatchProcessed()
            {
                PartitionId = partition.PartitionId,
                SessionId = messageBatch.SessionId,
                InstanceId = workItem.InstanceId,
                BatchStartPosition = messageBatch.BatchStartPosition,
                BatchLength = messageBatch.BatchLength,
                NewEvents = newOrchestrationRuntimeState.NewEvents.ToList(), // `NewEvents` in `newOrchestrationRuntimeState` may be mutated, so we copy to avoid a surprise change.
                WorkItemForReuse = cacheWorkItemForReuse ? orchestrationWorkItem : null,
                PackPartitionTaskMessages = partition.Settings.PackPartitionTaskMessages,
                PersistFirst = partition.Settings.PersistStepsFirst ? BatchProcessed.PersistFirstStatus.Required : BatchProcessed.PersistFirstStatus.NotRequired,
                OrchestrationStatus = state.OrchestrationStatus,
                ExecutionId = state.OrchestrationInstance.ExecutionId,
                ActivityMessages = (List<TaskMessage>)activityMessages,
                LocalMessages = localMessages,
                RemoteMessages = remoteMessages,
                TimerMessages = (List<TaskMessage>)timerMessages,
                Timestamp = state.LastUpdatedTime,
                DeleteInstance = newOrchestrationRuntimeState.IsImplicitDeletion(),
            };

            if (state.Status != orchestrationWorkItem.CustomStatus)
            {
                orchestrationWorkItem.CustomStatus = state.Status;
                batchProcessedEvent.CustomStatusUpdated = true;
                batchProcessedEvent.CustomStatus = state.Status;
            }

            this.workItemTraceHelper.TraceWorkItemCompleted(
                partition.PartitionId,
                orchestrationWorkItem.WorkItemType,
                messageBatch.WorkItemId,
                workItem.InstanceId,
                batchProcessedEvent.OrchestrationStatus,
                latencyMs,
                sequenceNumber);

            partition.SubmitEvent(batchProcessedEvent);

            if (this.workItemTraceHelper.TraceTaskMessages)
            {
                foreach (var taskMessage in batchProcessedEvent.LoopBackMessages())
                {
                    this.workItemTraceHelper.TraceTaskMessageSent(partition.PartitionId, taskMessage, messageBatch.WorkItemId, null, null);
                }
            }

            return Task.CompletedTask;
        }

        Task IOrchestrationService.AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // We can get here due to transient execution failures of the functions runtime.
            // In order to guarantee the work is done, we must enqueue a new work item.
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;
            var originalHistorySize = orchestrationWorkItem.OrchestrationRuntimeState.Events.Count - orchestrationWorkItem.OrchestrationRuntimeState.NewEvents.Count;
            var originalCustomStatus = orchestrationWorkItem.OrchestrationRuntimeState.Status;
            var originalHistory = orchestrationWorkItem.OrchestrationRuntimeState.Events.Take(originalHistorySize).ToList();
            var newWorkItem = new OrchestrationWorkItem(orchestrationWorkItem.Partition, orchestrationWorkItem.MessageBatch, originalHistory, originalCustomStatus);
            newWorkItem.Type = OrchestrationWorkItem.ExecutionType.ContinueFromHistory;
            newWorkItem.EventCount = originalHistory.Count;

            orchestrationWorkItem.Partition.EnqueueOrchestrationWorkItem(newWorkItem);

            return Task.CompletedTask;
        }

        Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.CompletedTask;
        }

        Task IOrchestrationService.RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        BehaviorOnContinueAsNew IOrchestrationService.EventBehaviourForContinueAsNew
            => this.Settings.EventBehaviourForContinueAsNew;

        bool IOrchestrationService.IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => this.Settings.MaxConcurrentOrchestratorFunctions;

        int IOrchestrationService.TaskOrchestrationDispatcherCount => this.Settings.OrchestrationDispatcherCount;


        /******************************/
        // Task activity methods
        /******************************/

        async Task<TaskActivityWorkItem> IOrchestrationService.LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            var nextActivityWorkItem = await this.ActivityWorkItemQueue.GetNext(receiveTimeout, cancellationToken).ConfigureAwait(false);

            if (nextActivityWorkItem != null)
            {
                if (nextActivityWorkItem.WaitForDequeueCountPersistence != null)
                {
                    await nextActivityWorkItem.WaitForDequeueCountPersistence.Task.ConfigureAwait(false);
                }

                this.workItemTraceHelper.TraceWorkItemStarted(
                    nextActivityWorkItem.Partition.PartitionId,
                    WorkItemTraceHelper.WorkItemType.Activity,
                    nextActivityWorkItem.WorkItemId,
                    nextActivityWorkItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nextActivityWorkItem.ExecutionType,
                    WorkItemTraceHelper.FormatMessageId(nextActivityWorkItem.TaskMessage, nextActivityWorkItem.OriginWorkItem));

                nextActivityWorkItem.StartedAt = this.workItemStopwatch.Elapsed.TotalMilliseconds;
            }

            return nextActivityWorkItem;
        }

        Task IOrchestrationService.AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            // put it back into the work queue
            this.ActivityWorkItemQueue.Add((ActivityWorkItem)workItem);
            return Task.CompletedTask;
        }

        Task IOrchestrationService.CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            var activityWorkItem = (ActivityWorkItem)workItem;
            var partition = activityWorkItem.Partition;
            var latencyMs = this.workItemStopwatch.Elapsed.TotalMilliseconds - activityWorkItem.StartedAt;

            var activityCompletedEvent = new ActivityCompleted()
            {
                PartitionId = activityWorkItem.Partition.PartitionId,
                ActivityId = activityWorkItem.ActivityId,
                OriginPartitionId = activityWorkItem.OriginPartition,
                ReportedLoad = this.ActivityWorkItemQueue.Load,
                Timestamp = DateTime.UtcNow,
                LatencyMs = latencyMs,
                Response = responseMessage,
            };

            if (partition.ErrorHandler.IsTerminated)
            {
                // we get here if the partition was terminated. The work is thrown away. 
                // It's unavoidable by design, but let's at least create a warning.
                this.workItemTraceHelper.TraceWorkItemDiscarded(
                    partition.PartitionId,
                    WorkItemTraceHelper.WorkItemType.Activity,
                    activityWorkItem.WorkItemId,
                    activityWorkItem.TaskMessage.OrchestrationInstance.InstanceId,
                    "",
                    "partition was terminated"
                   );

                return Task.CompletedTask;
            }

            this.workItemTraceHelper.TraceWorkItemCompleted(
                partition.PartitionId,
                WorkItemTraceHelper.WorkItemType.Activity,
                activityWorkItem.WorkItemId,
                activityWorkItem.TaskMessage.OrchestrationInstance.InstanceId,
                WorkItemTraceHelper.ActivityStatus.Completed,
                latencyMs,
                1);

            try
            {
                partition.SubmitEvent(activityCompletedEvent);
                this.workItemTraceHelper.TraceTaskMessageSent(partition.PartitionId, activityCompletedEvent.Response, activityWorkItem.WorkItemId, null, null);
            }
            catch (OperationCanceledException e)
            {
                // we get here if the partition was terminated. The work is thrown away. 
                // It's unavoidable by design, but let's at least create a warning.
                partition.ErrorHandler.HandleError(
                    nameof(IOrchestrationService.CompleteTaskActivityWorkItemAsync),
                    "Canceling already-completed activity work item because of partition termination",
                    e,
                    false,
                    true);
            }

            return Task.CompletedTask;
        }

        Task<TaskActivityWorkItem> IOrchestrationService.RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => this.Settings.MaxConcurrentActivityFunctions;

        int IOrchestrationService.TaskActivityDispatcherCount => this.Settings.ActivityDispatcherCount;

        EntityBackendProperties IEntityOrchestrationService.EntityBackendProperties => new EntityBackendProperties()
        {
            EntityMessageReorderWindow = TimeSpan.Zero,
            MaxConcurrentTaskEntityWorkItems = this.Settings.MaxConcurrentEntityFunctions,
            MaxEntityOperationBatchSize = this.Settings.MaxEntityOperationBatchSize,
            MaximumSignalDelayTime = TimeSpan.MaxValue,
            SupportsImplicitEntityDeletion = true,
            UseSeparateQueueForEntityWorkItems = this.Settings.UseSeparateQueueForEntityWorkItems,
        };
    }
}