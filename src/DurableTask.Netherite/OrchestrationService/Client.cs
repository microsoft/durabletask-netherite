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
        readonly CancellationToken shutdownToken;
        readonly ClientTraceHelper traceHelper;
        readonly string account;
        readonly Guid taskHubGuid;
        readonly WorkItemTraceHelper workItemTraceHelper;
        readonly Stopwatch workItemStopwatch;

        static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(5);

        public Guid ClientId { get; private set; }
        TransportAbstraction.ISender BatchSender { get; set; }

        long SequenceNumber; // for numbering requests that enter on this client

        readonly BatchTimer<PendingRequest> ResponseTimeouts;
        readonly ConcurrentDictionary<long, PendingRequest> ResponseWaiters;
        readonly Dictionary<(string,int), (MemoryStream, int)> Fragments;
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
            this.shutdownToken = shutdownToken;
            this.ResponseTimeouts = new BatchTimer<PendingRequest>(this.shutdownToken, this.Timeout, this.traceHelper.TraceTimerProgress);
            this.ResponseWaiters = new ConcurrentDictionary<long, PendingRequest>();
            this.Fragments = new Dictionary<(string,int), (MemoryStream, int)>();
            this.QueryResponses = new Dictionary<long, QueryResponseReceived>();
            this.ResponseTimeouts.Start("ClientTimer");
            this.workItemStopwatch = new Stopwatch();
            this.workItemStopwatch.Start();

            this.traceHelper.TraceProgress("Started");
        }

        public Task StopAsync()
        {
            this.traceHelper.TraceProgress("Stopped");
            return Task.CompletedTask;
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
                var index = (originalEventString, clientEvent.ReceiveChannel);

                if (this.traceHelper.LogLevelLimit == Microsoft.Extensions.Logging.LogLevel.Trace)
                {
                    this.traceHelper.TraceReceive(fragment, ClientTraceHelper.ResponseType.Fragment);
                }

                if (fragment.IsLast)
                {
                    (MemoryStream stream, int last) = this.Fragments[index];

                    if (last != fragment.Fragment)
                    {
                        throw new InvalidDataException($"wrong fragment sequence for event id={originalEventString}");
                    }

                    var reassembledEvent = FragmentationAndReassembly.Reassemble<ClientEvent>(stream, fragment);

                    this.Process(reassembledEvent);
                }
                else
                {
                    (MemoryStream, int) streamAndPosition;

                    if (fragment.Fragment == 0)
                    {
                        this.Fragments[index] = streamAndPosition = (new MemoryStream(), 0);
                    }
                    else
                    {
                        streamAndPosition = this.Fragments[index];
                    }
                    
                    if (streamAndPosition.Item2 != fragment.Fragment)
                    {
                        throw new InvalidDataException($"wrong fragment sequence for event id={originalEventString}");
                    }

                    streamAndPosition.Item1.Write(fragment.Bytes, 0, fragment.Bytes.Length);
                    streamAndPosition.Item2++;

                    this.Fragments[index] = streamAndPosition;
                }
            }
            else
            {
                this.Fragments.Remove((clientEvent.EventIdString, clientEvent.ReceiveChannel));

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
                        this.traceHelper.TraceReceive(queryResponseReceived, ClientTraceHelper.ResponseType.CompleteQ);
                        this.ProcessInternal(queryResponseReceived);
                    }
                    else
                    {
                        this.traceHelper.TraceReceive(queryResponseReceived, ClientTraceHelper.ResponseType.PartialQ);
                        this.QueryResponses[queryResponseReceived.RequestId] = queryResponseReceived;
                    }
                }
                else
                {
                    this.traceHelper.TraceReceive(clientEvent, ClientTraceHelper.ResponseType.Response);
                    this.ProcessInternal(clientEvent);
                }
            }
        }

        void ProcessInternal(ClientEvent clientEvent)
        {
            if (this.ResponseWaiters.TryRemove(clientEvent.RequestId, out var waiter))
            {
                waiter.Respond(clientEvent);
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

        Task<ClientEvent> PerformRequestWithTimeoutAndCancellation(CancellationToken token, IClientRequestEvent request, bool doneWhenSent)
        {
            var partitionEvent = (PartitionEvent)request;
            int timeoutId = this.ResponseTimeouts.GetFreshId();
            var pendingRequest = new PendingRequest(request.RequestId, request.EventId, partitionEvent.PartitionId, this, request.TimeoutUtc, timeoutId);
            this.ResponseWaiters.TryAdd(request.RequestId, pendingRequest);
            this.ResponseTimeouts.Schedule(request.TimeoutUtc, pendingRequest, timeoutId);

            DurabilityListeners.Register((Event)request, pendingRequest);

            this.Send(partitionEvent);

            return pendingRequest.Task;
        }

        internal class PendingRequest : TransportAbstraction.IDurabilityOrExceptionListener
        {
            readonly long requestId;
            readonly EventId partitionEventId;
            readonly uint partitionId;
            readonly Client client;
            readonly (DateTime due, int id) timeoutKey;
            readonly TaskCompletionSource<ClientEvent> continuation;
            readonly double startTime;

            static readonly TimeoutException timeoutException = new TimeoutException("Client request timed out.");

            public Task<ClientEvent> Task => this.continuation.Task;
            public (DateTime, int) TimeoutKey => this.timeoutKey;

            public string RequestId => $"{Client.GetShortId(this.client.ClientId)}-{this.requestId}"; // matches EventId

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

            var response = await this.PerformRequestWithTimeoutAndCancellation(this.shutdownToken, request, false).ConfigureAwait(false);
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

            return this.PerformRequestWithTimeoutAndCancellation(this.shutdownToken, request, true);
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
                var response = await this.PerformRequestWithTimeoutAndCancellation(cancellationToken, request, false).ConfigureAwait(false);
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

            var response = await this.PerformRequestWithTimeoutAndCancellation(this.shutdownToken, request, false).ConfigureAwait(false);
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

            var response = (HistoryResponseReceived)await this.PerformRequestWithTimeoutAndCancellation(this.shutdownToken, request, false).ConfigureAwait(false);
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
                cancellationToken);

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

        Task<TResult> RunPartitionQueries<TRequest, TResponse, TResult>(
           string[] partitionPositions,
           Func<TRequest> requestCreator,
           TResult initialResult,
           Func<TResult, TResponse, TResult> responseAggregator,
           Func<TResponse, string> continuationToken,
           CancellationToken cancellationToken)
           where TRequest : ClientRequestEventWithQuery
        {
            if (this.host.Settings.KeepInstanceIdsInMemory)
            {
                return this.RunPagedPartitionQueries(partitionPositions, requestCreator, initialResult, responseAggregator, cancellationToken);
            }
            else
            {
                return this.RunUnpagedPartitionQueries((uint partitionId) =>
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
                cancellationToken);
            }
        }

        async Task<TResult> RunUnpagedPartitionQueries<TRequest, TResponse, TResult>(
            Func<uint, TRequest> requestCreator,
            Func<IEnumerable<TResponse>, TResult> responseAggregator,
            CancellationToken cancellationToken)
            where TRequest : IClientRequestEvent
        {
            IEnumerable<Task<ClientEvent>> launchQueries()
            {
                for (uint partitionId = 0; partitionId < this.host.NumberPartitions; ++partitionId)
                {
                    yield return this.PerformRequestWithTimeoutAndCancellation(cancellationToken, requestCreator(partitionId), false);
                }
            }
            ClientEvent[] responses = await Task.WhenAll(launchQueries().ToList()).ConfigureAwait(false);
            return responseAggregator(responses.Cast<TResponse>());
        }

        public async Task<TResult> RunPagedPartitionQueries<TRequest, TResponse, TResult>(
           string[] partitionPositions,
           Func<TRequest> requestCreator,
           TResult initialResult,
           Func<TResult, TResponse, TResult> responseAggregator,
           CancellationToken cancellationToken)
        {

            throw new NotImplementedException();

            //string[] continuationTokens;

            //async Task<(int partitionId, IList<OrchestrationState> states, string continuationToken)> queryAsync(int partitionId)
            //{
            //    if (continuationTokens[partitionId] == null)
            //    {
            //        return (partitionId, System.Array.Empty<OrchestrationState>(), null);
            //    }
            //    else
            //    {
            //        var response = (QueryResponseReceived)await this.PerformRequestWithTimeoutAndCancellation(
            //            cancellationToken,
            //            new InstanceQueryReceived()
            //            {
            //                PartitionId = (uint)partitionId,
            //                ClientId = this.ClientId,
            //                RequestId = Interlocked.Increment(ref this.SequenceNumber),
            //                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            //                InstanceQuery = instanceQuery,
            //                ContinuationToken = continuationTokens[partitionId],
            //                PageSize = pageSize,PageTime = ...
            //            },
            //            false);

            //        return (partitionId, response.OrchestrationStates, response.ContinuationToken);
            //    }
            //}

            //var tasks = Enumerable.Range(0, (int)this.host.NumberPartitions).Select(i => queryAsync(i)).ToList();

            //await Task.WhenAll(tasks).ConfigureAwait(false);            // TODO consider masking partial errors 

            //string[] tokens = tasks.Select(task => task.Result.continuationToken).ToArray();

            //return new InstanceQueryResult()
            //{
            //    Instances = tasks.SelectMany(task => task.Result.states),
            //    ContinuationToken = tokens.Any(t => t != null) ? JsonConvert.SerializeObject(tokens) : null,
            //};
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

            return this.PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, true);
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

            var response = await this.PerformRequestWithTimeoutAndCancellation(this.shutdownToken, request, false).ConfigureAwait(false);
            return ((DeletionResponseReceived)response).NumberInstancesDeleted;
        }
    }
}
