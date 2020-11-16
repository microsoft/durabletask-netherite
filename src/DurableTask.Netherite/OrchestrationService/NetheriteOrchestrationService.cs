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

        internal Guid ServiceInstanceId { get; } = Guid.NewGuid();
        internal ILogger Logger { get; }
        internal ILoggerFactory LoggerFactory { get; }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"NetheriteOrchestrationService on {this.configuredTransport}Transport and {this.configuredStorage}Storage";
        }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public NetheriteOrchestrationService(NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.Settings = settings;
            TransportConnectionString.Parse(this.Settings.EventHubsConnectionString, out this.configuredStorage, out this.configuredTransport, out _);
            this.Logger = loggerFactory.CreateLogger(LoggerCategoryName);
            this.LoggerFactory = loggerFactory;
            this.StorageAccountName = this.configuredTransport == TransportConnectionString.TransportChoices.Memory
                ? this.Settings.StorageConnectionString
                : CloudStorageAccount.Parse(this.Settings.StorageConnectionString).Credentials.AccountName;

            EtwSource.Log.OrchestrationServiceCreated(this.ServiceInstanceId, this.StorageAccountName, this.Settings.HubName, this.Settings.WorkerId, TraceUtils.ExtensionVersion);
            this.Logger.LogInformation("NetheriteOrchestrationService created, workerId={workerId}, transport={transport}, storage={storage}", this.Settings.WorkerId, this.configuredTransport, this.configuredStorage);

            switch (this.configuredTransport)
            {
                case TransportConnectionString.TransportChoices.Memory:
                    this.taskHub = new Emulated.MemoryTransport(this, settings, this.Logger);
                    break;

                case TransportConnectionString.TransportChoices.EventHubs:
                    this.taskHub = new EventHubs.EventHubsTransport(this, settings, loggerFactory);
                    break;

                default:
                    throw new NotImplementedException("no such transport choice");
            }

            this.workItemTraceHelper = new WorkItemTraceHelper(loggerFactory, settings.WorkItemLogLevelLimit, this.StorageAccountName, this.Settings.HubName);

            if (this.configuredTransport != TransportConnectionString.TransportChoices.Memory)
                this.LoadMonitorService = new AzureLoadMonitorTable(settings.StorageConnectionString, settings.LoadInformationAzureTableName, settings.HubName);

            this.Logger.LogInformation(
                "trace generation limits: general={general} , transport={transport}, storage={storage}, events={events}; workitems={workitems}; etwEnabled={etwEnabled}; core.IsTraceEnabled={core}",
                settings.LogLevelLimit,
                settings.TransportLogLevelLimit,
                settings.StorageLogLevelLimit,
                settings.EventLogLevelLimit,
                settings.WorkItemLogLevelLimit,
                EtwSource.Log.IsEnabled(),
                DurableTask.Core.Tracing.DefaultEventSource.Log.IsTraceEnabled);
        }

        /******************************/
        // storage provider
        /******************************/

        IPartitionState IStorageProvider.CreatePartitionState()
        {
            switch (this.configuredStorage)
            {
                case TransportConnectionString.StorageChoices.Memory:
                    return new MemoryStorage(this.Logger);

                case TransportConnectionString.StorageChoices.Faster:
                    return new Faster.FasterStorage(this.Settings.StorageConnectionString, this.Settings.PremiumStorageConnectionString, this.Settings.HubName, this.LoggerFactory);

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
                    await Faster.FasterStorage.DeleteTaskhubStorageAsync(this.Settings.StorageConnectionString, this.Settings.HubName).ConfigureAwait(false);
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
                    await this.taskHub.CreateAsync().ConfigureAwait(false);
                }
            }
            else
            {
                await this.taskHub.CreateAsync().ConfigureAwait(false);
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
                    return;
                }

                this.Logger.LogInformation("NetheriteOrchestrationService is starting on workerId={workerId}", this.Settings.WorkerId);

                this.serviceShutdownSource = new CancellationTokenSource();

                this.ActivityWorkItemQueue = new WorkItemQueue<ActivityWorkItem>();
                this.OrchestrationWorkItemQueue = new WorkItemQueue<OrchestrationWorkItem>();

                LeaseTimer.Instance.DelayWarning = (int delay) =>
                    this.Logger.LogWarning("NetheriteOrchestrationService lease timer on workerId={workerId} is running {delay}s behind schedule", this.Settings.WorkerId, delay);

                if (!(this.LoadMonitorService is null))
                    this.LoadPublisher = new LoadPublisher(this.LoadMonitorService, this.serviceShutdownSource.Token, this.Logger);

                await this.taskHub.StartAsync().ConfigureAwait(false);

                System.Diagnostics.Debug.Assert(this.client != null, "Backend should have added client");
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.startupException = e;
                string message = $"NetheriteOrchestrationService failed to start: {e.Message}";
                EtwSource.Log.OrchestrationServiceError(this.StorageAccountName, message, e.ToString(), this.Settings.HubName, this.Settings.WorkerId, TraceUtils.ExtensionVersion);
                this.Logger.LogError("NetheriteOrchestrationService failed to start: {exception}", e);
                throw;
            }
        }

        /// <inheritdoc />
        public async Task StopAsync(bool isForced)
        {
            try
            {

                this.Logger.LogInformation("NetheriteOrchestrationService stopping, workerId={workerId} isForced={}", this.Settings.WorkerId, isForced);

                if (!this.Settings.KeepServiceRunning && this.serviceShutdownSource != null)
                {
                    this.serviceShutdownSource.Cancel();
                    this.serviceShutdownSource.Dispose();
                    this.serviceShutdownSource = null;

                    await this.taskHub.StopAsync(isForced).ConfigureAwait(false);

                    this.ActivityWorkItemQueue.Dispose();
                    this.OrchestrationWorkItemQueue.Dispose();

                    this.Logger.LogInformation("NetheriteOrchestrationService stopped, workerId={workerId}", this.Settings.WorkerId);
                    EtwSource.Log.OrchestrationServiceStopped(this.ServiceInstanceId, this.StorageAccountName, this.Settings.HubName, this.Settings.WorkerId, TraceUtils.ExtensionVersion);
                }
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                string message = $"NetheriteOrchestrationService failed to shut down: {e.Message}";
                EtwSource.Log.OrchestrationServiceError(this.StorageAccountName, message, e.ToString(), this.Settings.HubName, this.Settings.WorkerId, TraceUtils.ExtensionVersion);
                this.Logger.LogError("NetheriteOrchestrationService failed to shut down: {exception}", e);
                throw;
            }
        }

        /// <inheritdoc />
        public Task StopAsync() => ((IOrchestrationService)this).StopAsync(false);

        /// <inheritdoc/>
        public void Dispose() => this.taskHub.StopAsync(true);

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

        IStorageProvider TransportAbstraction.IHost.StorageProvider => this;

        IPartitionErrorHandler TransportAbstraction.IHost.CreateErrorHandler(uint partitionId)
        {
            return new PartitionErrorHandler((int) partitionId, this.Logger, this.Settings.LogLevelLimit, this.StorageAccountName, this.Settings.HubName);
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
                return null;
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
                    nextOrchestrationWorkItem.Type.ToString());
            }
     
            return nextOrchestrationWorkItem;
        }

        Task IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;
            var messageBatch = orchestrationWorkItem.MessageBatch;
            var partition = orchestrationWorkItem.Partition;

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

            if (outboundMessages != null)
            {
                foreach(TaskMessage taskMessage in outboundMessages)
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

            // if this orchestration is not done, and extended sessions are enabled, we keep the work item so we can reuse the execution cursor
            bool cacheWorkItemForReuse = partition.Settings.ExtendedSessionsEnabled && state.OrchestrationStatus == OrchestrationStatus.Running;

            BatchProcessed batchProcessedEvent = new BatchProcessed()
            {
                PartitionId = partition.PartitionId,
                SessionId = messageBatch.SessionId,
                InstanceId = workItem.InstanceId,
                BatchStartPosition = messageBatch.BatchStartPosition,
                BatchLength = messageBatch.BatchLength,
                NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                WorkItemForReuse = cacheWorkItemForReuse ? orchestrationWorkItem : null,
                State = state,
                ActivityMessages = (List<TaskMessage>)outboundMessages,
                LocalMessages = localMessages,
                RemoteMessages = remoteMessages,
                TimerMessages = (List<TaskMessage>)timerMessages,
                Timestamp = DateTime.UtcNow,
            };

            this.workItemTraceHelper.TraceWorkItemCompleted(
                partition.PartitionId,
                WorkItemTraceHelper.WorkItemType.Orchestration,
                messageBatch.WorkItemId,
                workItem.InstanceId,
                batchProcessedEvent.State.OrchestrationStatus,
                WorkItemTraceHelper.FormatMessageIdList(batchProcessedEvent.TracedTaskMessages));

            try
            {
                partition.SubmitInternalEvent(batchProcessedEvent);
            }
            catch(OperationCanceledException e)
            {
                // we get here if the partition was terminated. The work is thrown away. 
                // It's unavoidable by design, but let's at least create a warning.
                partition.ErrorHandler.HandleError(
                    nameof(IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync), 
                    "Canceling completed orchestration work item because of partition termination", 
                    e, 
                    false, 
                    true);
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
                    nextActivityWorkItem.ExecutionType);
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

            var activityCompletedEvent = new ActivityCompleted()
            {
                PartitionId = activityWorkItem.Partition.PartitionId,
                ActivityId = activityWorkItem.ActivityId,
                OriginPartitionId = activityWorkItem.OriginPartition,
                ReportedLoad = this.ActivityWorkItemQueue.Load,
                Timestamp = DateTime.UtcNow,
                Response = responseMessage,
            };

            this.workItemTraceHelper.TraceWorkItemCompleted(
                partition.PartitionId,
                WorkItemTraceHelper.WorkItemType.Activity,
                activityWorkItem.WorkItemId,
                activityWorkItem.TaskMessage.OrchestrationInstance.InstanceId,
                WorkItemTraceHelper.ActivityStatus.Completed,
                WorkItemTraceHelper.FormatMessageId(responseMessage, activityWorkItem.WorkItemId));

            try
            {
                partition.SubmitInternalEvent(activityCompletedEvent);
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