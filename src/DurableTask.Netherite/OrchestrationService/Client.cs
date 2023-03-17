// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Azure.Storage;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using static DurableTask.Netherite.TransportAbstraction;
    using static Microsoft.Azure.Amqp.Serialization.SerializableType;

    class Client : TransportAbstraction.IClient
    {
        readonly NetheriteOrchestrationService host;
        readonly ClientTraceHelper traceHelper;
        readonly string account;
        readonly Guid taskHubGuid;
        readonly WorkItemTraceHelper workItemTraceHelper;
        readonly Stopwatch workItemStopwatch;
        readonly CancellationTokenSource cts;

        static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(5);

        public Guid ClientId { get; private set; }
        TransportAbstraction.ISender BatchSender { get; set; }

        long SequenceNumber; // for numbering requests that enter on this client

        volatile bool allRemainingRequestsAreNowBeingCancelled = false; // set when entering the final stage of shutdown

        readonly BatchTimer<PendingRequest> ResponseTimeouts;
        readonly ConcurrentDictionary<long, PendingRequest> ResponseWaiters;
        readonly Dictionary<string, (MemoryStream, int)> Fragments;
        readonly Dictionary<long, QueryResponseReceived> QueryResponses;

        public static string GetShortId(Guid clientId) => clientId.ToString("N").Substring(0, 7);

        public Client(
            NetheriteOrchestrationService host,
            Guid clientId, 
            Guid taskHubGuid, 
            TransportAbstraction.ISender batchSender, 
            WorkItemTraceHelper workItemTraceHelper,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.ClientId = clientId;
            this.taskHubGuid = taskHubGuid;
            this.traceHelper = new ClientTraceHelper(host.LoggerFactory, host.Settings.ClientLogLevelLimit, host.StorageAccountName, host.Settings.HubName, this.ClientId);
            this.workItemTraceHelper = workItemTraceHelper;
            this.account = host.StorageAccountName;
            this.BatchSender = batchSender;
            this.cts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
            this.ResponseTimeouts = new BatchTimer<PendingRequest>(this.cts.Token, this.Timeout, this.traceHelper.TraceTimerProgress);
            this.ResponseWaiters = new ConcurrentDictionary<long, PendingRequest>();
            this.Fragments = new Dictionary<string, (MemoryStream, int)>();
            this.QueryResponses = new Dictionary<long, QueryResponseReceived>();
            this.ResponseTimeouts.Start("ClientTimer");
            this.workItemStopwatch = new Stopwatch();
            this.workItemStopwatch.Start();

            this.traceHelper.TraceProgress("Started");
        }

        public async Task StopAsync()
        {
            this.traceHelper.TraceProgress("Stopping");

            // cancel the token, if not already cancelled.
            this.cts.Cancel();

            await this.ResponseTimeouts.StopAsync();

            // We now enter the final stage of client shutdown, where we forcefully cancel
            // all requests that have not completed yet. We do this as late as possible in the shutdown
            // process, so that requests still have a chance to successfully complete as long as possible.
            this.allRemainingRequestsAreNowBeingCancelled = true;
            while (true)
            {
                var entry = this.ResponseWaiters.GetEnumerator();
                if (entry.MoveNext())
                {
                    entry.Current.Value.TryCancel(shutdownException);
                }
                else
                {
                    break;
                }
            }

            this.cts.Dispose();

            this.traceHelper.TraceProgress("Stopped");     
        }

        public void ReportTransportError(string message, Exception e)
        {
            this.traceHelper.TraceError("ReportTransportError", message, e);
        }

        public void Process(ClientEvent clientEvent)
        {
            if (clientEvent is ClientEventFragment fragment)
            {
                var originalEventString = fragment.OriginalEventId.ToString();
                var group = fragment.GroupId.HasValue
                  ? fragment.GroupId.Value.ToString()       // groups are now the way we track fragments
                  : $"{originalEventString}-{fragment.ReceiveChannel}";  // prior to introducing groups, we used event id and channel, which is not always good enough

                if (this.traceHelper.LogLevelLimit == Microsoft.Extensions.Logging.LogLevel.Trace)
                {
                    this.traceHelper.TraceReceive(fragment, ClientTraceHelper.ResponseType.Fragment);
                }

                if (fragment.IsLast)
                {
                    (MemoryStream stream, int last) = this.Fragments[group];

                    if (last != fragment.Fragment)
                    {
                        throw new InvalidDataException($"wrong fragment sequence for event id={originalEventString}");
                    }

                    var reassembledEvent = FragmentationAndReassembly.Reassemble<ClientEvent>(stream, fragment);

                    this.Fragments.Remove(group);

                    this.Process(reassembledEvent);
                }
                else
                {
                    (MemoryStream, int) streamAndPosition;

                    if (fragment.Fragment == 0)
                    {
                        this.Fragments[group] = streamAndPosition = (new MemoryStream(), 0);
                    }
                    else
                    {
                        streamAndPosition = this.Fragments[group];
                    }
                    
                    if (streamAndPosition.Item2 != fragment.Fragment)
                    {
                        throw new InvalidDataException($"wrong fragment sequence for event id={originalEventString}");
                    }

                    streamAndPosition.Item1.Write(fragment.Bytes, 0, fragment.Bytes.Length);
                    streamAndPosition.Item2++;

                    this.Fragments[group] = streamAndPosition;
                }
            }
            else
            {
                if (clientEvent is QueryResponseReceived queryResponseReceived)
                {
                    queryResponseReceived.DeserializeOrchestrationStates();

                    bool GotAllResults() => queryResponseReceived.Final == queryResponseReceived.OrchestrationStates.Count;

                    if (this.QueryResponses.TryGetValue(queryResponseReceived.RequestId, out QueryResponseReceived prev))
                    {
                        if (prev.Attempt < queryResponseReceived.Attempt)
                        {
                            // ignore the previous entry since we are processing a new attempt
                            this.QueryResponses.Remove(queryResponseReceived.RequestId);
                        }
                        else if (prev.Attempt > queryResponseReceived.Attempt)
                        {
                            // the response we just received is part of a superseded attempt, so we just ignore it altogether.
                            return;
                        }
                        else
                        {
                            // combine the previously stored states and the newly received ones
                            prev.OrchestrationStates.AddRange(queryResponseReceived.OrchestrationStates);
                            queryResponseReceived.OrchestrationStates = prev.OrchestrationStates;

                            // keep the final count and continuation token, if we received it in the previous message
                            if (prev.Final.HasValue)
                            {
                                queryResponseReceived.Final = prev.Final;
                                queryResponseReceived.ContinuationToken = prev.ContinuationToken;
                            }
                        }

                        if (GotAllResults())
                        {
                            this.QueryResponses.Remove(queryResponseReceived.RequestId);
                        }
                    }

                    if (GotAllResults())
                    {
                        this.ProcessInternal(queryResponseReceived);
                    }
                    else
                    {
                        this.traceHelper.TraceReceive(queryResponseReceived, ClientTraceHelper.ResponseType.Partial);
                        this.QueryResponses[queryResponseReceived.RequestId] = queryResponseReceived;
                    }
                }
                else
                {
                    this.ProcessInternal(clientEvent);
                }
            }
        }

        void ProcessInternal(ClientEvent clientEvent)
        {       
            if (this.ResponseWaiters.TryRemove(clientEvent.RequestId, out var waiter))
            {
                this.traceHelper.TraceReceive(clientEvent, ClientTraceHelper.ResponseType.Response);
                waiter.Respond(clientEvent);
            }
            else
            {
                this.traceHelper.TraceReceive(clientEvent, ClientTraceHelper.ResponseType.Obsolete);
            }
        }

        public void Send(PartitionEvent partitionEvent)
        {
            this.traceHelper.TraceSend(partitionEvent);
            this.BatchSender.Submit(partitionEvent);
        }

        void Timeout(List<PendingRequest> pendingRequests)
        {
            Parallel.ForEach(pendingRequests, pendingRequest => pendingRequest.TryTimeout());
        }

        // we align timeouts into buckets so we can process timeout storms more efficiently
        const long ticksPerBucket = 2 * TimeSpan.TicksPerSecond;
        DateTime GetTimeoutBucket(TimeSpan timeout) => new DateTime((((DateTime.UtcNow + timeout).Ticks / ticksPerBucket) * ticksPerBucket), DateTimeKind.Utc);

        Task<ClientEvent> PerformRequestWithTimeout(IClientRequestEvent request)
        {
            var partitionEvent = (PartitionEvent)request;
            int timeoutId = this.ResponseTimeouts.GetFreshId();
            var pendingRequest = new PendingRequest(request.RequestId, request.EventId, partitionEvent.PartitionId, this, request.TimeoutUtc, timeoutId);
            this.ResponseWaiters.TryAdd(request.RequestId, pendingRequest);
            this.ResponseTimeouts.Schedule(request.TimeoutUtc, pendingRequest, timeoutId);

            if (this.allRemainingRequestsAreNowBeingCancelled)
            {
                throw shutdownException;
            }

            DurabilityListeners.Register((Event)request, pendingRequest);

            this.Send(partitionEvent);

            return pendingRequest.Task;
        }

        async Task<ClientEvent> PerformRequestWithTimeoutAndCancellation(IClientRequestEvent request, CancellationToken token)
        {
            long requestId = request.RequestId;

            using CancellationTokenRegistration _ = token.Register(() =>
            {
                if (this.ResponseWaiters.TryGetValue(requestId, out PendingRequest request))
                {
                    var exception = new OperationCanceledException("Client request was cancelled by the application-provided cancellation token", token);
                    request.TryCancel(exception);
                }
            });

            var partitionEvent = (PartitionEvent)request;
            int timeoutId = this.ResponseTimeouts.GetFreshId();
            var pendingRequest = new PendingRequest(requestId, request.EventId, partitionEvent.PartitionId, this, request.TimeoutUtc, timeoutId);
            this.ResponseWaiters.TryAdd(requestId, pendingRequest);
            this.ResponseTimeouts.Schedule(request.TimeoutUtc, pendingRequest, timeoutId);

            if (this.allRemainingRequestsAreNowBeingCancelled)
            {
                throw shutdownException;
            }

            DurabilityListeners.Register((Event)request, pendingRequest);

            this.Send(partitionEvent);

            return await pendingRequest.Task.ConfigureAwait(false);
        }

        static readonly TimeoutException timeoutException = new TimeoutException("Client request timed out.");
        static readonly OperationCanceledException shutdownException = new OperationCanceledException("Client request was cancelled because host is shutting down.");

        internal class PendingRequest : TransportAbstraction.IDurabilityOrExceptionListener
        {
            readonly long requestId;
            readonly EventId partitionEventId;
            readonly uint partitionId;
            readonly Client client;
            readonly (DateTime due, int id) timeoutKey;
            readonly TaskCompletionSource<ClientEvent> continuation;
            readonly double startTime;


            public Task<ClientEvent> Task => this.continuation.Task;
            public (DateTime, int) TimeoutKey => this.timeoutKey;

            public PendingRequest(long requestId, EventId partitionEventId, uint partitionId, Client client, DateTime due, int timeoutId)
            {
                this.requestId = requestId;
                this.partitionId = partitionId;
                this.partitionEventId = partitionEventId;
                this.client = client;
                this.timeoutKey = (due, timeoutId);
                this.continuation = new TaskCompletionSource<ClientEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
                this.startTime = this.client.workItemStopwatch.Elapsed.TotalMilliseconds;
            }

            public void Respond(ClientEvent evt)
            {
                this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                this.continuation.TrySetResult(evt);
            }

            void TransportAbstraction.IDurabilityListener.ConfirmDurable(Event evt)
            {
                if (evt is ClientTaskMessagesReceived request)
                {
                    // we create a separate trace message for each destination partition
                    foreach (var group in request.TaskMessages.GroupBy((message) => message.OrchestrationInstance.InstanceId))
                    {
                        double delay = this.client.workItemStopwatch.Elapsed.TotalMilliseconds - this.startTime;
                        string workItemId = request.WorkItemId;

                        this.client.workItemTraceHelper.TraceWorkItemCompleted(
                            request.PartitionId,
                            WorkItemTraceHelper.WorkItemType.Client,
                            workItemId,
                            group.Key,
                            WorkItemTraceHelper.ClientStatus.Send,
                            delay,
                            request.TaskMessages.Length);

                        foreach (var message in request.TaskMessages)
                        {
                            this.client.workItemTraceHelper.TraceTaskMessageSent(request.PartitionId, message, workItemId, null, delay);
                        }
                    }

                    // this request is considered completed at the time of durability
                    // so we generate the response now
                    if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                    {
                        this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                        this.continuation.TrySetResult(null); // task finishes when the send has been confirmed, no result is returned
                    }
                }
                else if (evt is CreationRequestReceived creationRequestReceived)
                {
                    double delay = this.client.workItemStopwatch.Elapsed.TotalMilliseconds - this.startTime;
                    string workItemId = creationRequestReceived.WorkItemId;

                    this.client.workItemTraceHelper.TraceWorkItemCompleted(
                        creationRequestReceived.PartitionId,
                        WorkItemTraceHelper.WorkItemType.Client,
                        workItemId,
                        creationRequestReceived.InstanceId,
                        WorkItemTraceHelper.ClientStatus.Create,
                        delay,
                        1);

                    this.client.workItemTraceHelper.TraceTaskMessageSent(creationRequestReceived.PartitionId, creationRequestReceived.TaskMessage, workItemId, null, delay);
                }
            }

            void TransportAbstraction.IDurabilityOrExceptionListener.ReportException(Event evt, Exception e)
            {
                if (evt is ClientTaskMessagesReceived request)
                {
                    // we create a separate trace message for each destination partition
                    foreach (var group in request.TaskMessages.GroupBy((message) => message.OrchestrationInstance.InstanceId))
                    {
                        this.client.workItemTraceHelper.TraceWorkItemDiscarded(
                            request.PartitionId,
                            WorkItemTraceHelper.WorkItemType.Client,
                            request.WorkItemId,
                            group.Key,
                            "",
                            $"send failed with {e.GetType().FullName}");
                    }

                    if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                    {
                        this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                        this.continuation.TrySetException(e); // task finishes with exception
                    }
                }
                else if (evt is CreationRequestReceived creationRequestReceived)
                {
                    this.client.workItemTraceHelper.TraceWorkItemDiscarded(
                        creationRequestReceived.PartitionId,
                        WorkItemTraceHelper.WorkItemType.Client,
                        creationRequestReceived.WorkItemId,
                        creationRequestReceived.InstanceId,
                        "",
                        $"send failed with {e.GetType().FullName}");
                }            
            }

            public void TryTimeout()
            {
                this.client.traceHelper.TraceTimerProgress($"firing ({this.timeoutKey.due:o},{this.timeoutKey.id})");
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var pendingRequest))
                {
                    this.client.traceHelper.TraceRequestTimeout(pendingRequest.partitionEventId, pendingRequest.partitionId);
                    this.continuation.TrySetException(timeoutException);
                }
            }

            public void TryCancel(OperationCanceledException exception)
            {
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var pendingRequest))
                {
                    this.client.traceHelper.TraceTimerProgress($"cancelling ({this.timeoutKey.due:o},{this.timeoutKey.id})");
                    this.continuation.TrySetException(exception);
                }
            }
        }

        /******************************/
        // orchestration client methods
        /******************************/

        public async Task CreateTaskOrchestrationAsync(uint partitionId, TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            ExecutionStartedEvent executionStartedEvent = creationMessage.Event as ExecutionStartedEvent;
            if (executionStartedEvent == null)
            {
                throw new ArgumentException($"Only {nameof(EventType.ExecutionStarted)} messages are supported.", nameof(creationMessage));
            }

            var request = new CreationRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessage = creationMessage,
                DedupeStatuses = dedupeStatuses,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            this.workItemTraceHelper.TraceWorkItemStarted(
                partitionId,
                WorkItemTraceHelper.WorkItemType.Client,
                request.WorkItemId,
                creationMessage.OrchestrationInstance.InstanceId,
                "CreateOrchestration",
                WorkItemTraceHelper.FormatEmptyMessageIdList());

            var response = await this.PerformRequestWithTimeout(request).ConfigureAwait(false);
            var creationResponseReceived = (CreationResponseReceived)response;
            if (!creationResponseReceived.Succeeded)
            {
                // An instance in this state already exists.
                if (this.host.Settings.ThrowExceptionOnInvalidDedupeStatus)
                {
                    throw new InvalidOperationException($"An Orchestration instance with the status {creationResponseReceived.ExistingInstanceOrchestrationStatus} already exists.");
                }
            }
        }

        public Task SendTaskOrchestrationMessageBatchAsync(uint partitionId, IEnumerable<TaskMessage> messages)
        {
            var request = new ClientTaskMessagesReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessages = messages.ToArray(),
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            // we number the messages in this batch in order to create unique message ids for tracing purposes
            int sequenceNumber = 0;
            for(int i = 0; i < request.TaskMessages.Length; i++)
            {
                request.TaskMessages[i].SequenceNumber = sequenceNumber++;
            }

            // we create a separate trace message for each destination partition
            foreach (var group in request.TaskMessages.GroupBy((message) => message.OrchestrationInstance.InstanceId))
            {
                this.workItemTraceHelper.TraceWorkItemStarted(
                    partitionId,
                    WorkItemTraceHelper.WorkItemType.Client,
                    request.WorkItemId,
                    group.Key,
                    "SendMessages",
                    WorkItemTraceHelper.FormatEmptyMessageIdList());
            }

            return this.PerformRequestWithTimeout(request);
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            uint partitionId,
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new WaitRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                ExecutionId = executionId,
                Timestamp = DateTime.UtcNow,
                TimeoutUtc = this.GetTimeoutBucket(timeout),
            };

            try
            {
                var response = await this.PerformRequestWithTimeout(request).ConfigureAwait(false);
                return ((WaitResponseReceived)response)?.OrchestrationState;
            }
            catch (TimeoutException)
            {
                return null; // to match semantics of other backends, wait returns null when timing out
            }
        }

        public async Task<OrchestrationState> GetOrchestrationStateAsync(uint partitionId, string instanceId, bool fetchInput = true, bool fetchOutput = true)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new StateRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                IncludeInput = fetchInput,
                IncludeOutput = fetchOutput,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            var response = await this.PerformRequestWithTimeout(request).ConfigureAwait(false);
            return ((StateResponseReceived)response)?.OrchestrationState;
        }

        public async Task<(string executionId, IList<HistoryEvent> history)> GetOrchestrationHistoryAsync(uint partitionId, string instanceId)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new HistoryRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            var response = (HistoryResponseReceived)await this.PerformRequestWithTimeout(request).ConfigureAwait(false);
            return (response?.ExecutionId, response?.History);
        }

        public Task<List<OrchestrationState>> GetOrchestrationStateAsync(CancellationToken cancellationToken)
            => this.RunPartitionQueries(
                this.GetInitialPositions(), 
                () => new InstanceQueryReceived() {
                    InstanceQuery = new InstanceQuery(),
                },
                new List<OrchestrationState>(),
                (List<OrchestrationState> list, QueryResponseReceived response) =>
                {
                    list.AddRange(response.OrchestrationStates);
                    return list;
                },
                (QueryResponseReceived response) => response.ContinuationToken,
                pageSize: 500,
                keepGoingUntilDone: true,
                cancellationToken);

        public Task<List<OrchestrationState>> GetOrchestrationStateAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo,
                    IEnumerable<OrchestrationStatus> runtimeStatus, string instanceIdPrefix, CancellationToken cancellationToken = default)
            => this.RunPartitionQueries(
                this.GetInitialPositions(), 
                () => new InstanceQueryReceived() {
                    InstanceQuery = new InstanceQuery(
                        runtimeStatus?.ToArray(), 
                        createdTimeFrom?.ToUniversalTime(),
                        createdTimeTo?.ToUniversalTime(),
                        instanceIdPrefix,
                        fetchInput: true),
                },
                new List<OrchestrationState>(),
                (List<OrchestrationState> list, QueryResponseReceived response) =>
                {
                    list.AddRange(response.OrchestrationStates);
                    return list;
                },
                (QueryResponseReceived response) => response.ContinuationToken,
                pageSize: 500,
                keepGoingUntilDone: true,
                cancellationToken);

        public Task<int> PurgeInstanceHistoryAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default)
            => this.RunPartitionQueries(
                this.GetInitialPositions(),
                () => new PurgeRequestReceived()
                {
                    InstanceQuery = new InstanceQuery(
                        runtimeStatus?.ToArray(),
                        createdTimeFrom?.ToUniversalTime(),
                        createdTimeTo?.ToUniversalTime(),
                        null,
                        fetchInput: false)
                    { PrefetchHistory = true },
                },
                0,
                (int sum, PurgeResponseReceived response) => sum + response.NumberInstancesPurged,
                (PurgeResponseReceived response) => response.ContinuationToken,
                pageSize: 500,
                keepGoingUntilDone: true,
                cancellationToken);

        public async Task<InstanceQueryResult> QueryOrchestrationStatesAsync(InstanceQuery instanceQuery, int pageSize, string continuationToken, CancellationToken cancellationToken)
        {
            var positions = this.ConvertContinuationTokenToPositions(continuationToken);

            List<OrchestrationState> instances = await this.RunPartitionQueries(
                positions,
                () => new InstanceQueryReceived()
                {
                    InstanceQuery = instanceQuery,
                },
                new List<OrchestrationState>(),
                (List<OrchestrationState> list, QueryResponseReceived response) =>
                {
                    list.AddRange(response.OrchestrationStates);
                    return list;
                },
                (QueryResponseReceived response) => response.ContinuationToken,
                pageSize,
                pageSize == 0,
                cancellationToken).ConfigureAwait(false);

            continuationToken = this.ConvertPositionsToContinuationToken(positions);

            return new InstanceQueryResult()
            {
                Instances = instances,
                ContinuationToken = continuationToken,
            };
        }

        string[] GetInitialPositions()
        {
            string[] positions = new string[this.host.NumberPartitions];
            for (int i = 0; i < positions.Length; i++)
            {
                positions[i] = String.Empty;
            }
            return positions;
        }

        string[] ConvertContinuationTokenToPositions(string continuationToken)
        {
            if (string.IsNullOrEmpty(continuationToken))
            {
                return this.GetInitialPositions();
            }
            else
            {
                string[] positions;

                try
                {
                    positions = JsonConvert.DeserializeObject<string[]>(continuationToken);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("invalid continuation token: failed to parse: ", e);
                }

                if (positions.Length != this.host.NumberPartitions)
                {
                    throw new ArgumentException("invalid continuation token: wrong number of partition");
                }

                if (positions.Count(s => s != null) == 0)
                {
                    throw new ArgumentException("invalid continuation token: no targets");
                }

                return positions;
            }
        }

        string ConvertPositionsToContinuationToken(string[] positions)
        {
            return positions.Any(t => t != null) ? JsonConvert.SerializeObject(positions) : null;
        }

        async Task<TResult> RunPartitionQueries<TRequest, TResponse, TResult>(
           string[] partitionPositions,
           Func<TRequest> requestCreator,
           TResult initialResult,
           Func<TResult, TResponse, TResult> responseAggregator,
           Func<TResponse, string> continuationToken,
           int pageSize,
           bool keepGoingUntilDone,
           CancellationToken cancellationToken)
           where TRequest : ClientRequestEventWithQuery
           where TResponse : ClientEvent, IPagedResponse
        {
            string clientQueryId = $"Q{Interlocked.Increment(ref this.SequenceNumber)}";
            string PrintPartitionPositions() => string.Join(",", partitionPositions.Select(s => s ?? "null"));

            if (!this.host.Settings.KeepInstanceIdsInMemory)
            {
                pageSize = 0; // paging is not supported
            }

            this.traceHelper.TraceProgress($"Query {clientQueryId} type={typeof(TRequest).Name} paging={pageSize > 0} starting at {PrintPartitionPositions()}");

            var stopwatch = Stopwatch.StartNew();
            try
            {
                TResult result;

                if (pageSize > 0)
                {
                    result = await this.RunPagedPartitionQueries(
                        clientQueryId,
                        partitionPositions,
                        requestCreator,
                        initialResult,
                        responseAggregator,
                        pageSize,
                        keepGoingUntilDone,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    result = await this.RunUnpagedPartitionQueries(
                        clientQueryId,
                        partitionPositions,
                        (uint partitionId) =>
                        {
                            var request = requestCreator();
                            request.PartitionId = partitionId;
                            request.ClientId = this.ClientId;
                            request.RequestId = Interlocked.Increment(ref this.SequenceNumber);
                            request.PageSize = 0;
                            request.TimeoutUtc = this.GetTimeoutBucket(TimeSpan.FromMinutes(60));
                            return request;
                        },
                        (IEnumerable<TResponse> responses) =>
                        {
                            TResult result = initialResult;
                            int i = 0;
                            foreach (var response in responses)
                            {
                                result = responseAggregator(result, response);
                                partitionPositions[i++] = continuationToken(response);
                            }
                            return result;
                        },
                        cancellationToken).ConfigureAwait(false);
                }

                this.traceHelper.TraceProgress($"Query {clientQueryId} type={typeof(TRequest).Name} completed after {stopwatch.Elapsed.TotalSeconds:F2}s at {PrintPartitionPositions()}");
                return result;
            }
            catch (Exception exception)
            {
                this.traceHelper.TraceError("RunPartitionQueries", $"Query {clientQueryId} type={typeof(TRequest).Name} failed after {stopwatch.Elapsed.TotalSeconds:F2}s", exception);
                throw;
            }
        }

        async Task<TResult> RunUnpagedPartitionQueries<TRequest, TResponse, TResult>(
            string clientQueryId,
            string[] partitionPositions, 
            Func<uint, TRequest> requestCreator,
            Func<IEnumerable<TResponse>, TResult> responseAggregator,
            CancellationToken cancellationToken)
            where TRequest : ClientRequestEventWithQuery
            where TResponse : ClientEvent, IPagedResponse
        {           
            async Task<TResponse> QueryPartition(uint partitionId)
            {
                var stopwatch = Stopwatch.StartNew();
                if (partitionPositions[partitionId] != null)
                {
                    var request = requestCreator(partitionId);
                    request.ContinuationToken = partitionPositions[partitionId];
                    var response = (TResponse)await this.PerformRequestWithTimeoutAndCancellation(request, cancellationToken).ConfigureAwait(false);
                    partitionPositions[partitionId] = response.ContinuationToken;
                    this.traceHelper.TraceQueryProgress(clientQueryId, request.EventIdString, partitionId, stopwatch.Elapsed, request.PageSize, response.Count, response.ContinuationToken);
                    return response;
                }
                else
                {
                    // we have already reached the end of this partition
                    return null;
                }
            }

            var tasks = Enumerable.Range(0, (int) this.host.NumberPartitions).Select(i => QueryPartition((uint) i)).ToList();
            ClientEvent[] responses = await Task.WhenAll(tasks).ConfigureAwait(false);
            return responseAggregator(responses.Where(r => r != null).Cast<TResponse>());
        }

        public async Task<TResult> RunPagedPartitionQueries<TRequest, TResponse, TResult>(
            string clientQueryId,
            string[] partitionPositions,
            Func<TRequest> requestCreator,
            TResult initialResult,
            Func<TResult, TResponse, TResult> responseAggregator,
            int pageSize,
            bool keepGoingUntilDone,
            CancellationToken cancellationToken)
            where TRequest : ClientRequestEventWithQuery
            where TResponse: ClientEvent, IPagedResponse
        {
            TResult currentResult = initialResult;
            var aggregationLock = new object();

            // query each partition
            var tasks = new Task[partitionPositions.Length];
            for (uint i = 0; i < tasks.Length; i++)
            {
                tasks[i] = QueryPartition(i);
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // return the aggregated result
            return currentResult;
           
            async Task QueryPartition(uint partitionId)
            {
                int retries = 5;
                TimeSpan retryDelay = TimeSpan.FromSeconds(2);

                Task BackOffAsync()
                {
                    retries--;
                    retryDelay += retryDelay;
                    return Task.Delay(retryDelay);
                }

                void ResetRetries()
                {
                    retries = 5;
                    retryDelay = TimeSpan.FromSeconds(2);
                }

                bool hasMore = (partitionPositions[partitionId] != null);

                while (hasMore && !cancellationToken.IsCancellationRequested)
                {
                    hasMore = await GetNextPageAsync().ConfigureAwait(false);

                    if (!keepGoingUntilDone)
                    {
                        return; // we take only the first page from each partition
                    }
                }

                async Task<bool> GetNextPageAsync()
                {
                    var request = requestCreator();
                    request.PartitionId = partitionId;
                    request.ClientId = this.ClientId;
                    request.RequestId = Interlocked.Increment(ref this.SequenceNumber);
                    request.PageSize = pageSize;
                    request.TimeoutUtc = this.GetTimeoutBucket(Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30));
                    request.ContinuationToken = partitionPositions[partitionId];

                    try
                    {
                        if (request.ContinuationToken == null)
                        {
                            throw new InvalidDataException($"query {clientQueryId} is issuing query for already-completed partition id {partitionId}");
                        }

                        var stopwatch = Stopwatch.StartNew();
                        var response = (TResponse)await this.PerformRequestWithTimeoutAndCancellation(request, cancellationToken).ConfigureAwait(false);
                        this.traceHelper.TraceQueryProgress(clientQueryId, request.EventIdString, partitionId, stopwatch.Elapsed, request.PageSize, response.Count, response.ContinuationToken);

                        ResetRetries();

                        lock (aggregationLock)
                        {
                            currentResult = responseAggregator(currentResult, response);
                        }

                        if (response.ContinuationToken == null)
                        {
                            partitionPositions[partitionId] = null;
                            return false;
                        }

                        int progress = response.ContinuationToken.CompareTo(partitionPositions[partitionId]);

                        if (progress > 0)
                        {
                            partitionPositions[partitionId] = response.ContinuationToken;
                            return true;
                        }
                        else if (progress < 0)
                        {
                            throw new InvalidDataException($"query {clientQueryId} received invalid continuation token for {request.EventId}");
                        }
                        else if (retries > 0)
                        {
                            await BackOffAsync().ConfigureAwait(false);
                            return true;
                        }
                        else
                        {
                            throw new TimeoutException($"query {clientQueryId} did not make progress in time on partition {partitionId}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return false;
                    }
                    catch (Exception) when (retries > 0)
                    {
                        await BackOffAsync().ConfigureAwait(false);
                        return true;
                    }
                }
            }
        }

        public Task ForceTerminateTaskOrchestrationAsync(uint partitionId, string instanceId, string message)
        {
            var taskMessages = new[] { new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, message)
            } };

            var request = new ClientTaskMessagesReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessages = taskMessages,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            return this.PerformRequestWithTimeout(request);
        }

        public async Task<int> DeleteAllDataForOrchestrationInstance(uint partitionId, string instanceId)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new DeletionRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            var response = await this.PerformRequestWithTimeout(request).ConfigureAwait(false);
            return ((DeletionResponseReceived)response).NumberInstancesDeleted;
        }
    }
}
