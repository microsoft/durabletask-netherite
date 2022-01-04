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
        IOrchestrationService, 
        IOrchestrationServiceClient, 
        TransportAbstraction.IHost,
        IStorageProvider,
        IDisposable
    {
        readonly ITaskHub taskHub;
        readonly TransportConnectionString.StorageChoices configuredStorage;
        readonly TransportConnectionString.TransportChoices configuredTransport;

        readonly WorkItemTraceHelper workItemTraceHelper;

        readonly Stopwatch workItemStopwatch = new Stopwatch();

        /// <summary>
        /// The logger category prefix used for all ILoggers in this backend.
        /// </summary>
        public const string LoggerCategoryName = "DurableTask.Netherite";

        CancellationTokenSource serviceShutdownSource;
        Exception startupException;

        //internal Dictionary<uint, Partition> Partitions { get; private set; }
        internal Client CheckedClient
        {
            get
            {
                if (this.client == null)
                {
                    throw new InvalidOperationException($"failed to start: {this.startupException.Message}", this.startupException);
                }
                return this.client;
            }
        }
        Client client;

        internal ILoadMonitorService LoadMonitorService { get; private set; }

        internal NetheriteOrchestrationServiceSettings Settings { get; private set; }
        internal uint NumberPartitions { get; private set; }
        uint TransportAbstraction.IHost.NumberPartitions { set => this.NumberPartitions = value; }
        internal string StorageAccountName { get; private set; }

        internal WorkItemQueue<ActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkItemQueue<OrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        internal LoadPublisher LoadPublisher { get; private set; }

        internal ILoggerFactory LoggerFactory { get; }
        internal OrchestrationServiceTraceHelper TraceHelper { get; private set; }

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

                if (this.configuredStorage == TransportConnectionString.StorageChoices.Faster)
                {
                    // force dll load here so exceptions are observed early
                    var _ = System.Threading.Channels.Channel.CreateBounded<DateTime>(10);

                    // throw descriptive exception if run on 32bit platform
                    if (!Environment.Is64BitProcess)
                    {
                        throw new NotSupportedException("Netherite backend requires 64bit, but current process is 32bit.");
                    }
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
                    + $"events={settings.EventLogLevelLimit}; workitems={settings.WorkItemLogLevelLimit}; etwEnabled={EtwSource.Log.IsEnabled()}; "
                    + $"core.IsTraceEnabled={DurableTask.Core.Tracing.DefaultEventSource.Log.IsTraceEnabled}");
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
                monitor = new ScalingMonitor(
                    this.Settings.ResolvedStorageConnectionString, 
                    this.Settings.ResolvedTransportConnectionString, 
                    this.Settings.LoadInformationAzureTableName, 
                    this.Settings.HubName,
                    this.TraceHelper.TraceScaleRecommendation,
                    this.TraceHelper.Logger);
                return true;
            }
            else
            {
                monitor = null;
                return false;
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
                    return new Faster.FasterStorage(this.Settings.ResolvedStorageConnectionString, this.Settings.ResolvedPageBlobStorageConnectionString, this.Settings.UseLocalDirectoryForPartitionStorage, this.Settings.HubName, this.LoggerFactory);

                default:
                    throw new NotImplementedException("no such storage choice");
            }
        }

        async Task IStorageProvider.DeleteAllPartitionStatesAsync()
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
                        this.Settings.HubName).ConfigureAwait(false);
                    break;

                default:
                    throw new NotImplementedException("no such storage choice");
            }
        }

        /******************************/
        // management methods
        /******************************/

        /// <inheritdoc />
        public async Task CreateAsync() => await ((IOrchestrationService)this).CreateAsync(true).ConfigureAwait(false);

        /// <inheritdoc />
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            if (await this.taskHub.ExistsAsync().ConfigureAwait(false))
            {
                if (recreateInstanceStore)
                {
                    await this.taskHub.DeleteAsync().ConfigureAwait(false);
                    await this.taskHub.CreateIfNotExistsAsync().ConfigureAwait(false);
                }
            }
            else
            {
                await this.taskHub.CreateIfNotExistsAsync().ConfigureAwait(false);
            }

            if (!(this.LoadMonitorService is null))
                await this.LoadMonitorService.CreateIfNotExistsAsync(CancellationToken.None).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync() => await ((IOrchestrationService)this).CreateAsync(false).ConfigureAwait(false);

        /// <inheritdoc />
        public async Task DeleteAsync()
        {
            await this.taskHub.DeleteAsync().ConfigureAwait(false);

            if (!(this.LoadMonitorService is null))
                await this.LoadMonitorService.DeleteIfExistsAsync(CancellationToken.None).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore) => await this.DeleteAsync().ConfigureAwait(false);

        /// <inheritdoc />
        public async Task StartAsync()
        {
            try
            {
                if (this.serviceShutdownSource != null)
                {
                    // we left the service running. No need to start it again.
                    this.TraceHelper.TraceProgress("Reusing");
                    return;
                }

                this.TraceHelper.TraceProgress("Starting");

                this.serviceShutdownSource = new CancellationTokenSource();

                this.ActivityWorkItemQueue = new WorkItemQueue<ActivityWorkItem>();
                this.OrchestrationWorkItemQueue = new WorkItemQueue<OrchestrationWorkItem>();

                LeaseTimer.Instance.DelayWarning = (int delay) =>
                    this.TraceHelper.TraceWarning($"Lease timer is running {delay}s behind schedule");

                if (!(this.LoadMonitorService is null))
                {
                    this.TraceHelper.TraceProgress("Starting Load Publisher");
                    this.LoadPublisher = new LoadPublisher(this.LoadMonitorService, CancellationToken.None, this.TraceHelper);
                }

                this.TraceHelper.TraceProgress("Starting TaskHub");
                await this.taskHub.StartAsync();

                if (this.Settings.PartitionCount != this.NumberPartitions)
                {
                    this.TraceHelper.TraceWarning($"Ignoring configuration setting partitionCount={this.Settings.PartitionCount} because existing TaskHub has {this.NumberPartitions} partitions");
                }

                System.Diagnostics.Debug.Assert(this.client != null, "Backend should have added client");

                this.TraceHelper.TraceProgress($"Started partitionCount={this.NumberPartitions}");
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

        /// <inheritdoc />
        public async Task StopAsync(bool quickly)
        {
            try
            {
                this.TraceHelper.TraceProgress($"Stopping quickly={quickly}");

                if (!this.Settings.KeepServiceRunning && this.serviceShutdownSource != null)
                {
                    this.serviceShutdownSource.Cancel();
                    this.serviceShutdownSource.Dispose();
                    this.serviceShutdownSource = null;

                    await this.taskHub.StopAsync();

                    this.ActivityWorkItemQueue.Dispose();
                    this.OrchestrationWorkItemQueue.Dispose();
                }

                this.TraceHelper.TraceProgress("Stopped cleanly");
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

        /// <inheritdoc />
        public Task StopAsync() => ((IOrchestrationService)this).StopAsync(false);

        /// <inheritdoc/>
        public void Dispose() => this.taskHub.StopAsync();

        /// <summary>
        /// Computes the partition for the given instance.
        /// </summary>
        /// <param name="instanceId">The instance id.</param>
        /// <returns>The partition id.</returns>
        public uint GetPartitionId(string instanceId)
        {
            // if the instance id ends with !nn, where nn is a two-digit number, it indicates explicit partition placement
            if (instanceId.Length >= 3 
                && instanceId[instanceId.Length - 3] == '!'
                && uint.TryParse(instanceId.Substring(instanceId.Length - 2), out uint nn))
            {
                var partitionId = nn % this.NumberPartitions;
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
        Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage)
            => this.CheckedClient.CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                null);

        /// <inheritdoc />
        Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
            => this.CheckedClient.CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                dedupeStatuses);

        /// <inheritdoc />
        Task IOrchestrationServiceClient.SendTaskOrchestrationMessageAsync(TaskMessage message)
            => this.CheckedClient.SendTaskOrchestrationMessageBatchAsync(
                this.GetPartitionId(message.OrchestrationInstance.InstanceId),
                new[] { message });

        /// <inheritdoc />
        Task IOrchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
            => messages.Length == 0
                ? Task.CompletedTask
                : Task.WhenAll(messages
                    .GroupBy(tm => this.GetPartitionId(tm.OrchestrationInstance.InstanceId))
                    .Select(group => this.CheckedClient.SendTaskOrchestrationMessageBatchAsync(group.Key, group))
                    .ToList());

        /// <inheritdoc />
        Task<OrchestrationState> IOrchestrationServiceClient.WaitForOrchestrationAsync(
                string instanceId,
                string executionId,
                TimeSpan timeout,
                CancellationToken cancellationToken) 
            => this.CheckedClient.WaitForOrchestrationAsync(
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
            var state = await this.CheckedClient.GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, true).ConfigureAwait(false);
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
            var state = await this.CheckedClient.GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, true).ConfigureAwait(false);
            return state != null 
                ? (new[] { state }) 
                : (new OrchestrationState[0]);
        }

        /// <inheritdoc />
        Task IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(
                string instanceId, 
                string message)
            => this.CheckedClient.ForceTerminateTaskOrchestrationAsync(this.GetPartitionId(instanceId), instanceId, message);

        /// <inheritdoc />
        async Task<string> IOrchestrationServiceClient.GetOrchestrationHistoryAsync(
            string instanceId, 
            string executionId)
        {
            (string actualExecutionId, IList<HistoryEvent> history) = 
                await this.CheckedClient.GetOrchestrationHistoryAsync(this.GetPartitionId(instanceId), instanceId).ConfigureAwait(false);

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
        Task IOrchestrationServiceClient.PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc, 
            OrchestrationStateTimeRangeFilterType 
            timeRangeFilterType)
        {
            if (timeRangeFilterType != OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter)
            {
                throw new NotSupportedException("Purging is supported only for Orchestration created time filter.");
            }

            return this.CheckedClient.PurgeInstanceHistoryAsync(thresholdDateTimeUtc, null, null);
        }

        /// <summary>
        /// Gets the current state of an instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        /// <param name="fetchOutput">If set, fetch and return the output for the orchestration instance.</param>
        /// <returns>The state of the instance, or null if not found.</returns>
        public Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, bool fetchInput = true, bool fetchOutput = true)
        {
            return this.CheckedClient.GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId, fetchInput, fetchOutput);
        }

        /// <summary>
        /// Gets the state of all orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public Task<IList<OrchestrationState>> GetAllOrchestrationStatesAsync(CancellationToken cancellationToken)
            => this.CheckedClient.GetOrchestrationStateAsync(cancellationToken);

        /// <summary>
        /// Gets the state of selected orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? CreatedTimeFrom = default,
                                                                          DateTime? CreatedTimeTo = default,
                                                                          IEnumerable<OrchestrationStatus> RuntimeStatus = default,
                                                                          string InstanceIdPrefix = default,
                                                                          CancellationToken CancellationToken = default)
            => this.CheckedClient.GetOrchestrationStateAsync(CreatedTimeFrom, CreatedTimeTo, RuntimeStatus, InstanceIdPrefix, CancellationToken);


        /// <summary>
        /// Purge history for an orchestration with a specified instance id.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        public Task<int> PurgeInstanceHistoryAsync(string instanceId)
            => this.CheckedClient.DeleteAllDataForOrchestrationInstance(this.GetPartitionId(instanceId), instanceId);

        /// <summary>
        /// Purge history for orchestrations that match the specified parameters.
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Purges history grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Purges history less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        public Task<int> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
            => this.CheckedClient.PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus);

        /// <summary>
        /// Query orchestration instance states.
        /// </summary>
        /// <param name="instanceQuery">The query to perform.</param>
        /// <param name="pageSize">The page size.</param>
        /// <param name="continuationToken">The continuation token.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The result of the query.</returns>
        public Task<InstanceQueryResult> QueryOrchestrationStatesAsync(InstanceQuery instanceQuery, int pageSize, string continuationToken, CancellationToken cancellationToken)
            => this.CheckedClient.QueryOrchestrationStatesAsync(instanceQuery, pageSize, continuationToken, cancellationToken);

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

            // DurableTask.Core keeps the original runtime state in the work item until after this call returns
            // but we want it to contain the latest runtime state now (otherwise IsExecutableInstance returns incorrect results)
            // so we update it now.
            workItem.OrchestrationRuntimeState = newOrchestrationRuntimeState;

            // all continue as new requests are processed immediately (DurableTask.Core always uses "fast" continue-as-new)
            // so by the time we get here, it is not a continue as new
            partition.Assert(continuedAsNewMessage == null);
            partition.Assert(workItem.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew);

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

            if (state.Status != orchestrationWorkItem.PreStatus)
            {
                batchProcessedEvent.CustomStatusUpdated = true;
                batchProcessedEvent.CustomStatus = state.Status;
            }

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
            var originalHistory = orchestrationWorkItem.OrchestrationRuntimeState.Events.Take(originalHistorySize).ToList();
            var newWorkItem = new OrchestrationWorkItem(orchestrationWorkItem.Partition, orchestrationWorkItem.MessageBatch, originalHistory);
            newWorkItem.Type = OrchestrationWorkItem.ExecutionType.ContinueFromHistory;
            newWorkItem.HistorySize = originalHistory.Count;

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
    }
}