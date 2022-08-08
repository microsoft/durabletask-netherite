// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Faster;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.Storage;
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
        DurableTask.Netherite.IOrchestrationServiceQueryClient,
        TransportAbstraction.IHost,
        IStorageProvider
    {
        readonly ITaskHub taskHub;
        readonly TransportConnectionString.StorageChoices configuredStorage;
        readonly TransportConnectionString.TransportChoices configuredTransport;
        readonly MemoryTracker memoryTracker;

        readonly WorkItemTraceHelper workItemTraceHelper;

        readonly Stopwatch workItemStopwatch = new Stopwatch();

        /// <summary>
        /// The logger category prefix used for all ILoggers in this backend.
        /// </summary>
        public const string LoggerCategoryName = "DurableTask.Netherite";

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

        internal ILoadMonitorService LoadMonitorService { get; private set; }

        internal NetheriteOrchestrationServiceSettings Settings { get; private set; }
        internal uint NumberPartitions { get; private set; }
        uint TransportAbstraction.IHost.NumberPartitions { set => this.NumberPartitions = value; }
        internal string PathPrefix { get;  private set; }
        string TransportAbstraction.IHost.PathPrefix { set => this.PathPrefix = value; }
        internal string StorageAccountName { get; private set; }

        internal WorkItemQueue<ActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkItemQueue<OrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        internal LoadPublisher LoadPublisher { get; private set; }

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
            return $"NetheriteOrchestrationService on {this.configuredTransport}Transport and {this.configuredStorage}Storage, {configuration} build";
        }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public NetheriteOrchestrationService(NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.LoggerFactory = loggerFactory;
            this.Settings = settings;
            this.TraceHelper = new OrchestrationServiceTraceHelper(loggerFactory, settings.LogLevelLimit, settings.WorkerId, settings.HubName);
            this.workItemTraceHelper = new WorkItemTraceHelper(loggerFactory, settings.WorkItemLogLevelLimit, settings.HubName);
           
            try
            {
                this.TraceHelper.TraceProgress("Reading configuration for transport and storage providers");
                TransportConnectionString.Parse(this.Settings.ResolvedTransportConnectionString, out this.configuredStorage, out this.configuredTransport);
                this.StorageAccountName = this.configuredStorage == TransportConnectionString.StorageChoices.Memory
                    ? "Memory"
                    : CloudStorageAccount.Parse(this.Settings.ResolvedStorageConnectionString).Credentials.AccountName;
                
                // set the account name in the trace helpers
                this.TraceHelper.StorageAccountName = this.workItemTraceHelper.StorageAccountName = this.StorageAccountName;

                this.TraceHelper.TraceCreated(Environment.ProcessorCount, this.configuredTransport, this.configuredStorage);

                if (settings.RempTracer != null)
                {
                    settings.RempTracer?.WorkerHeader($"{settings.HubName}.{settings.WorkerId}", WorkItemTraceHelper.GetRempGroups(settings));
                    this.workItemTraceHelper.RempTracer = new Remp.RempSinkWithErrorHandler(settings.RempTracer, (Exception e) => this.TraceHelper.TraceError("exception in RempTracer", e));
                }

                if (this.configuredStorage == TransportConnectionString.StorageChoices.Faster)
                {
                    // force the loading of potentially problematic dll dependencies here so exceptions are observed early
                    var _a = System.Threading.Channels.Channel.CreateBounded<DateTime>(10);
                    bool _c = System.Runtime.CompilerServices.Unsafe.AreSame(ref _a, ref _a);

                    // throw descriptive exception if run on 32bit platform
                    if (!Environment.Is64BitProcess)
                    {
                        throw new NotSupportedException("Netherite backend requires 64bit, but current process is 32bit.");
                    }

                    this.memoryTracker = new MemoryTracker((long) (settings.InstanceCacheSizeMB ?? 400) * 1024 * 1024);
                }

                switch (this.configuredTransport)
                {
                    case TransportConnectionString.TransportChoices.Memory:
                        this.taskHub = new Emulated.MemoryTransport(this, settings, this.TraceHelper.Logger);
                        break;

                    case TransportConnectionString.TransportChoices.EventHubs:
                        this.taskHub = new EventHubs.EventHubsTransport(this, settings, loggerFactory);
                        break;

                    default:
                        throw new NotImplementedException("no such transport choice");
                }


                if (this.configuredTransport != TransportConnectionString.TransportChoices.Memory)
                {
                    this.TraceHelper.TraceProgress("Creating LoadMonitor Service");
                    if (!string.IsNullOrEmpty(settings.LoadInformationAzureTableName))
                    {
                        this.LoadMonitorService = new AzureTableLoadMonitor(settings.ResolvedStorageConnectionString, settings.LoadInformationAzureTableName, settings.HubName);
                    }
                    else
                    {
                        this.LoadMonitorService = new AzureBlobLoadMonitor(settings.ResolvedStorageConnectionString, settings.HubName);
                    }
                }

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
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.TraceHelper.TraceError("Could not create NetheriteOrchestrationService", e);
                throw;
            }
        }

        /// <summary>
        /// Get a scaling monitor for autoscaling.
        /// </summary>
        /// <param name="monitor">The returned scaling monitor.</param>
        /// <returns>true if autoscaling is supported, false otherwise</returns>
        public bool TryGetScalingMonitor(out ScalingMonitor monitor)
        {
            if (this.configuredStorage == TransportConnectionString.StorageChoices.Faster
                && this.configuredTransport == TransportConnectionString.TransportChoices.EventHubs)
            {
                try
                {
                    monitor = new ScalingMonitor(
                        this.Settings.ResolvedStorageConnectionString,
                        this.Settings.ResolvedTransportConnectionString,
                        this.Settings.LoadInformationAzureTableName,
                        this.Settings.HubName,
                        this.TraceHelper.TraceScaleRecommendation,
                        this.TraceHelper.TraceProgress,
                        this.TraceHelper.TraceError);
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
        // storage provider
        /******************************/

        IPartitionState IStorageProvider.CreatePartitionState()
        {
            switch (this.configuredStorage)
            {
                case TransportConnectionString.StorageChoices.Memory:
                    return new MemoryStorage(this.TraceHelper.Logger);

                case TransportConnectionString.StorageChoices.Faster:
                    return new Faster.FasterStorage(this.Settings, this.PathPrefix, this.memoryTracker, this.LoggerFactory);

                default:
                    throw new NotImplementedException("no such storage choice");
            }
        }

        async Task IStorageProvider.DeleteTaskhubAsync(string pathPrefix)
        {
            if (!(this.LoadMonitorService is null))
                await this.LoadMonitorService.DeleteIfExistsAsync(CancellationToken.None).ConfigureAwait(false);

            switch (this.configuredStorage)
            {
                case TransportConnectionString.StorageChoices.Memory:
                    await Task.Delay(10).ConfigureAwait(false);
                    break;

                case TransportConnectionString.StorageChoices.Faster:
                    await Faster.FasterStorage.DeleteTaskhubStorageAsync(
                        this.Settings.ResolvedStorageConnectionString, 
                        this.Settings.ResolvedPageBlobStorageConnectionString, 
                        this.Settings.UseLocalDirectoryForPartitionStorage, 
                        this.Settings.HubName,
                        pathPrefix).ConfigureAwait(false);
                    break;

                default:
                    throw new NotImplementedException("no such storage choice");
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
            if (await this.taskHub.ExistsAsync())
            {
                if (recreateInstanceStore)
                {
                    this.TraceHelper.TraceProgress("Creating");

                    await this.taskHub.DeleteAsync();
                    await this.taskHub.CreateIfNotExistsAsync();
                }
            }
            else
            {
                await this.taskHub.CreateIfNotExistsAsync();
            }

            if (!(this.LoadMonitorService is null))
                await this.LoadMonitorService.CreateIfNotExistsAsync(CancellationToken.None);
        }

        /// <inheritdoc />
        async Task IOrchestrationService.CreateIfNotExistsAsync() => await ((IOrchestrationService)this).CreateAsync(false);

        /// <inheritdoc />
        async Task IOrchestrationService.DeleteAsync()
        {
            await this.taskHub.DeleteAsync();

            if (!(this.LoadMonitorService is null))
                await this.LoadMonitorService.DeleteIfExistsAsync(CancellationToken.None);
        }

        /// <inheritdoc />
        async Task IOrchestrationService.DeleteAsync(bool deleteInstanceStore) => await ((IOrchestrationService)this).DeleteAsync();

        /// <inheritdoc />
        Task IOrchestrationService.StartAsync()
        {
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
            while (true)
            {
                var currentTransition = this.currentTransition;
                var currentState = await currentTransition;

                if (currentState == ServiceState.None)
                {
                    var greenLight = new TaskCompletionSource<bool>();
                    var startTask = this.StartClientAsync(greenLight.Task);
                    var nextTransition = Interlocked.CompareExchange<Task<ServiceState>>(ref this.currentTransition, startTask, currentTransition);
                    greenLight.SetResult(nextTransition == currentTransition);

                    continue;
                }

                if (currentState == ServiceState.Client)
                {
                    if (clientOnly)
                    {
                        return;
                    }

                    var greenLight = new TaskCompletionSource<bool>();
                    var startTask = this.StartWorkersAsync(greenLight.Task);
                    var nextTransition = Interlocked.CompareExchange<Task<ServiceState>>(ref this.currentTransition, startTask, currentTransition);
                    greenLight.SetResult(nextTransition == currentTransition);

                    continue;
                }

                return;
            }
        }

        async Task<ServiceState> StartClientAsync(Task<bool> greenLight)
        {
            if (!await greenLight) return ServiceState.None; 

            try
            {
               this.TraceHelper.TraceProgress("Starting Client");

                if (this.Settings.TestHooks != null)
                {
                    this.TraceHelper.TraceProgress(this.Settings.TestHooks.ToString());
                }

                this.serviceShutdownSource = new CancellationTokenSource();

                await this.taskHub.StartClientAsync();

                System.Diagnostics.Debug.Assert(this.client != null, "Backend should have added client");
               
                this.checkedClient = this.client;

                this.TraceHelper.TraceProgress($"Started client");

                return ServiceState.Client;
            }
            catch (Exception e) when (!Utils.IsFatal(e))
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
        
        async Task<ServiceState> StartWorkersAsync(Task<bool> greenLight)
        {
            if (!await greenLight) return ServiceState.Client;

            try
            {
                System.Diagnostics.Debug.Assert(this.client != null, "Backend should have added client");

                this.TraceHelper.TraceProgress("Starting Workers");

                this.ActivityWorkItemQueue = new WorkItemQueue<ActivityWorkItem>();
                this.OrchestrationWorkItemQueue = new WorkItemQueue<OrchestrationWorkItem>();

                LeaseTimer.Instance.DelayWarning = (int delay) =>
                    this.TraceHelper.TraceWarning($"Lease timer is running {delay}s behind schedule");

                if (!(this.LoadMonitorService is null))
                {
                    this.TraceHelper.TraceProgress("Starting Load Publisher");
                    this.LoadPublisher = new LoadPublisher(this.LoadMonitorService, CancellationToken.None, this.TraceHelper);
                }

                await this.taskHub.StartWorkersAsync();

                if (this.Settings.PartitionCount != this.NumberPartitions)
                {
                    this.TraceHelper.TraceWarning($"Ignoring configuration setting partitionCount={this.Settings.PartitionCount} because existing TaskHub has {this.NumberPartitions} partitions");
                }

                if (this.threadWatcher == null)
                {
                    this.threadWatcher = new Timer(this.WatchThreads, null, 0, 120000);
                }

                this.TraceHelper.TraceProgress($"Started partitionCount={this.NumberPartitions}");

                return ServiceState.Full;
            }
            catch (Exception e) when (!Utils.IsFatal(e))
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

        async Task<ServiceState> TryStopAsync(bool quickly)
        {
            try
            {
                this.TraceHelper.TraceProgress($"Stopping quickly={quickly}");

                this.OnStopping?.Invoke();

                this.checkedClient = null;
                this.client = null;

                if (this.serviceShutdownSource != null)
                {
                    this.serviceShutdownSource.Cancel();
                    this.serviceShutdownSource.Dispose();
                    this.serviceShutdownSource = null;

                    await this.taskHub.StopAsync();

                    this.ActivityWorkItemQueue.Dispose();
                    this.OrchestrationWorkItemQueue.Dispose();
                }

                this.threadWatcher?.Dispose();
                this.threadWatcher = null;

                this.TraceHelper.TraceProgress("Stopped cleanly");

                return ServiceState.None;
            }
            catch (Exception e) when (!Utils.IsFatal(e))
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

        TransportAbstraction.IClient TransportAbstraction.IHost.AddClient(Guid clientId, Guid taskHubGuid, TransportAbstraction.ISender batchSender)
        {
            System.Diagnostics.Debug.Assert(this.client == null, "Backend should create only 1 client");

            this.client = new Client(this, clientId, taskHubGuid, batchSender, this.workItemTraceHelper, this.serviceShutdownSource.Token);
            return this.client;
        }

        TransportAbstraction.IPartition TransportAbstraction.IHost.AddPartition(uint partitionId, TransportAbstraction.ISender batchSender)
        {
            var partition = new Partition(this, partitionId, this.GetPartitionId, this.GetNumberPartitions, batchSender, this.Settings, this.StorageAccountName,
                this.ActivityWorkItemQueue, this.OrchestrationWorkItemQueue, this.LoadPublisher, this.workItemTraceHelper);

            return partition;
        }

        TransportAbstraction.ILoadMonitor TransportAbstraction.IHost.AddLoadMonitor(Guid taskHubGuid, TransportAbstraction.ISender batchSender)
        {
            return new LoadMonitor(this, taskHubGuid, batchSender);
        }

        IStorageProvider TransportAbstraction.IHost.StorageProvider => this;

        IPartitionErrorHandler TransportAbstraction.IHost.CreateErrorHandler(uint partitionId)
        {
            return new PartitionErrorHandler((int) partitionId, this.TraceHelper.Logger, this.Settings.LogLevelLimit, this.StorageAccountName, this.Settings.HubName);
        }

        /******************************/
        // client methods
        /******************************/

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage)
            => await (await this.GetClientAsync()).CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                null);

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
            => await (await this.GetClientAsync()).CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                dedupeStatuses);

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.SendTaskOrchestrationMessageAsync(TaskMessage message)
            => await (await this.GetClientAsync()).SendTaskOrchestrationMessageBatchAsync(
                this.GetPartitionId(message.OrchestrationInstance.InstanceId),
                new[] { message });

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            var client = await this.GetClientAsync();
            if (messages.Length != 0)
            {
                await Task.WhenAll(messages
                    .GroupBy(tm => this.GetPartitionId(tm.OrchestrationInstance.InstanceId))
                    .Select(group => client.SendTaskOrchestrationMessageBatchAsync(group.Key, group))
                    .ToList());
            }
        }
           

        /// <inheritdoc />
        async Task<OrchestrationState> IOrchestrationServiceClient.WaitForOrchestrationAsync(
                string instanceId,
                string executionId,
                TimeSpan timeout,
                CancellationToken cancellationToken) 
            => await (await this.GetClientAsync()).WaitForOrchestrationAsync(
                this.GetPartitionId(instanceId),
                instanceId,
                executionId,
                timeout,
                cancellationToken);

        /// <inheritdoc />
        async Task<OrchestrationState> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId, 
            string executionId)
        {
            var state = await (await this.GetClientAsync()).GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, true).ConfigureAwait(false);
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
            var state = await (await this.GetClientAsync()).GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, true).ConfigureAwait(false);
            return state != null 
                ? (new[] { state }) 
                : (new OrchestrationState[0]);
        }

        /// <inheritdoc />
        async Task IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(
                string instanceId, 
                string message)
            => await (await this.GetClientAsync()).ForceTerminateTaskOrchestrationAsync(this.GetPartitionId(instanceId), instanceId, message);

        /// <inheritdoc />
        async Task<string> IOrchestrationServiceClient.GetOrchestrationHistoryAsync(
            string instanceId, 
            string executionId)
        {
            var client = await this.GetClientAsync();
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

            await (await this.GetClientAsync()).PurgeInstanceHistoryAsync(thresholdDateTimeUtc, null, null);
        }

        /// <inheritdoc />
        async Task<OrchestrationState> IOrchestrationServiceQueryClient.GetOrchestrationStateAsync(string instanceId, bool fetchInput, bool fetchOutput)
        {
            return await (await this.GetClientAsync()).GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, fetchInput, fetchOutput);
        }

        /// <inheritdoc />
        async Task<IList<OrchestrationState>> IOrchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken cancellationToken)
            => await (await this.GetClientAsync()).GetOrchestrationStateAsync(cancellationToken);

        /// <inheritdoc />
        async Task<IList<OrchestrationState>> IOrchestrationServiceQueryClient.GetOrchestrationStateAsync(DateTime? CreatedTimeFrom, DateTime? CreatedTimeTo, IEnumerable<OrchestrationStatus> RuntimeStatus, string InstanceIdPrefix, CancellationToken CancellationToken)
            => await (await this.GetClientAsync()).GetOrchestrationStateAsync(CreatedTimeFrom, CreatedTimeTo, RuntimeStatus, InstanceIdPrefix, CancellationToken);

        /// <inheritdoc />
        async Task<int> IOrchestrationServiceQueryClient.PurgeInstanceHistoryAsync(string instanceId)
            => await (await this.GetClientAsync()).DeleteAllDataForOrchestrationInstance(this.GetPartitionId(instanceId), instanceId);

        /// <inheritdoc />
        async Task<int> IOrchestrationServiceQueryClient.PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
            => await (await this.GetClientAsync()).PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus);

        /// <inheritdoc />
        async Task<InstanceQueryResult> IOrchestrationServiceQueryClient.QueryOrchestrationStatesAsync(InstanceQuery instanceQuery, int pageSize, string continuationToken, CancellationToken cancellationToken)
            => await (await this.GetClientAsync()).QueryOrchestrationStatesAsync(instanceQuery, pageSize, continuationToken, cancellationToken);

        /// <inheritdoc />
        async Task<PurgeResult> IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(string instanceId)
            => new PurgeResult(await (await this.GetClientAsync()).DeleteAllDataForOrchestrationInstance(this.GetPartitionId(instanceId), instanceId));

        /// <inheritdoc />
        async Task<PurgeResult> IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
            => new PurgeResult(await (await this.GetClientAsync()).PurgeInstanceHistoryAsync(purgeInstanceFilter.CreatedTimeFrom, purgeInstanceFilter.CreatedTimeTo, purgeInstanceFilter.RuntimeStatus));


        /******************************/
        // Task orchestration methods
        /******************************/

        async Task<TaskOrchestrationWorkItem> IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            var nextOrchestrationWorkItem = await this.OrchestrationWorkItemQueue.GetNext(receiveTimeout, cancellationToken).ConfigureAwait(false);

            if (nextOrchestrationWorkItem != null) 
            {
                nextOrchestrationWorkItem.MessageBatch.WaitingSince = null;

                this.workItemTraceHelper.TraceWorkItemStarted(
                    nextOrchestrationWorkItem.Partition.PartitionId, 
                    WorkItemTraceHelper.WorkItemType.Orchestration,
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
                    WorkItemTraceHelper.WorkItemType.Orchestration,
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
                NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
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
            };

            if (state.Status != orchestrationWorkItem.CustomStatus)
            {
                orchestrationWorkItem.CustomStatus = state.Status;
                batchProcessedEvent.CustomStatusUpdated = true;
                batchProcessedEvent.CustomStatus = state.Status;
            }

            this.workItemTraceHelper.RempTracer?.WorkItem(
                timeStamp: DateTime.UtcNow.Ticks,
                workItemId: messageBatch.WorkItemId,
                group: WorkItemTraceHelper.RempGroupOrchestration,
                latencyMs: latencyMs,
                consumedMessages: messageBatch.TracedMessages.Select(x => new Remp.RempFormat.NamedPayload()
                {
                    Id = WorkItemTraceHelper.FormatMessageId(x.Item1, x.Item2),
                    NumBytes = Serializer.GetMessageSize(x.Item1),
                }),
                producedMessages: batchProcessedEvent.ProducedMessages().Select(taskMessage => new Remp.RempFormat.NamedPayload()
                {
                    Id = WorkItemTraceHelper.FormatMessageId(taskMessage, messageBatch.WorkItemId),
                    NumBytes = Serializer.GetMessageSize(taskMessage),
                }),
                allowSpeculation: messageBatch.AllowSpeculation,
                instanceState: new Remp.RempFormat.InstanceState()
                {
                    InstanceId = workItem.InstanceId,
                    Updated = Serializer.GetStateSize(newOrchestrationRuntimeState, state),
                    Delta = (newOrchestrationRuntimeState.OrchestrationInstance.ExecutionId == orchestrationWorkItem.PreviousExecutionId) ? Serializer.GetDeltaSize(batchProcessedEvent) : null,
                }
            );

            this.workItemTraceHelper.TraceWorkItemCompleted(
                partition.PartitionId,
                WorkItemTraceHelper.WorkItemType.Orchestration,
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
            newWorkItem.PreviousExecutionId = orchestrationWorkItem.PreviousExecutionId;

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
                    await nextActivityWorkItem.WaitForDequeueCountPersistence.Task;
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

            this.workItemTraceHelper.RempTracer?.WorkItem(
                timeStamp: DateTime.UtcNow.Ticks,
                workItemId: activityWorkItem.WorkItemId,
                group: WorkItemTraceHelper.RempGroupActivity,
                latencyMs: latencyMs,
                consumedMessages: new[] { new Remp.RempFormat.NamedPayload()
                {
                    Id = WorkItemTraceHelper.FormatMessageId(activityWorkItem.TaskMessage, activityWorkItem.OriginWorkItem),
                    NumBytes = Serializer.GetMessageSize(activityWorkItem.TaskMessage),
                }},
                producedMessages: new[] { new Remp.RempFormat.NamedPayload()
                {
                    Id = WorkItemTraceHelper.FormatMessageId(activityCompletedEvent.Response, activityWorkItem.WorkItemId),
                    NumBytes = Serializer.GetMessageSize(activityCompletedEvent.Response),
                }},
                allowSpeculation: true,
                instanceState: null
            );

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
    }
}