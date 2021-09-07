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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

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
        readonly Dictionary<string, MemoryStream> Fragments;

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
            this.traceHelper = new ClientTraceHelper(host.Logger, host.Settings.LogLevelLimit, host.StorageAccountName, host.Settings.HubName, this.ClientId);
            this.workItemTraceHelper = workItemTraceHelper;
            this.account = host.StorageAccountName;
            this.BatchSender = batchSender;
            this.shutdownToken = shutdownToken;
            this.ResponseTimeouts = new BatchTimer<PendingRequest>(this.shutdownToken, this.Timeout, this.traceHelper.TraceTimerProgress);
            this.ResponseWaiters = new ConcurrentDictionary<long, PendingRequest>();
            this.Fragments = new Dictionary<string, MemoryStream>();
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
            if (!(clientEvent is ClientEventFragment fragment))
            {
                this.ProcessInternal(clientEvent);
            }
            else
            {
                var originalEventString = fragment.OriginalEventId.ToString();

                if (!fragment.IsLast)
                {
                    if (!this.Fragments.TryGetValue(originalEventString, out var stream))
                    {
                        this.Fragments[originalEventString] = stream = new MemoryStream();
                    }
                    stream.Write(fragment.Bytes, 0, fragment.Bytes.Length);
                }
                else
                {
                    var reassembledEvent = FragmentationAndReassembly.Reassemble<ClientEvent>(this.Fragments[originalEventString], fragment);
                    this.Fragments.Remove(fragment.EventIdString);

                    this.ProcessInternal(reassembledEvent);
                }
            }
        }

        void ProcessInternal(ClientEvent clientEvent)
        {
            this.traceHelper.TraceReceive(clientEvent);
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
                this.continuation = new TaskCompletionSource<ClientEvent>();
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
                Timestamp = DateTime.UtcNow,
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
                TimeoutUtc = this.GetTimeoutBucket(timeout),
            };

            var response = await this.PerformRequestWithTimeoutAndCancellation(cancellationToken, request, false).ConfigureAwait(false);
            return ((WaitResponseReceived)response)?.OrchestrationState;
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

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(CancellationToken cancellationToken)
            => this.RunUnpagedPartitionQueries(
                new InstanceQuery(),
                cancellationToken);

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo,
                    IEnumerable<OrchestrationStatus> runtimeStatus, string instanceIdPrefix, CancellationToken cancellationToken = default)
            => this.RunUnpagedPartitionQueries(
                new InstanceQuery(
                        runtimeStatus?.ToArray(), 
                        createdTimeFrom?.ToUniversalTime(),
                        createdTimeTo?.ToUniversalTime(),
                        instanceIdPrefix,
                        fetchInput: true),
                cancellationToken);

        async Task<IList<OrchestrationState>> RunUnpagedPartitionQueries(
           InstanceQuery instanceQuery,
           CancellationToken cancellationToken)
        {
            IEnumerable<Task<ClientEvent>> launchQueries()
            {
                for (uint partitionId = 0; partitionId < this.host.NumberPartitions; ++partitionId)
                {
                    yield return this.PerformRequestWithTimeoutAndCancellation(
                        cancellationToken,
                        new InstanceQueryReceived()
                        {
                            PartitionId = partitionId,
                            ClientId = this.ClientId,
                            RequestId = Interlocked.Increment(ref this.SequenceNumber),
                            TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                            InstanceQuery = instanceQuery,
                            ContinuationToken = null,
                            PageSize = 0,
                        },
                        false);
                }
            }
            ClientEvent[] responses = await Task.WhenAll(launchQueries().ToList()).ConfigureAwait(false);
            return responses.SelectMany(response => ((QueryResponseReceived)response).OrchestrationStates).ToList();
        }

        [JsonObject]
        class ContinuationToken
        {
            public JObject FToken { get; set; }
            public string Remainder { get; set; } // bitvector

            // ftoken and remainder are interpreted as follows
            // (null,     "1"     )   continue at start of last partition
            // (non-null,  ""     )   continue in middle of last partition
            // (null,     "1bbbbb")   continue at start of sixth-from-end partition
            // (non-null,  "bbbbb")   continue in middle of sixth-from-end partition
        }

        public async Task<InstanceQueryResult> QueryOrchestrationStatesAsync(
            InstanceQuery instanceQuery,
            int pageSize,
            string continuationTokenString,
            CancellationToken cancellationToken)
        {
            if (continuationTokenString == null)
            {
                int numberPartitions = (int)this.host.NumberPartitions;
                int[] pageSizes = Enumerable
                    .Range(0, numberPartitions)
                    .Select(i => i == 0   // query pattern is (100, 10, 10, ..., 10)
                        ? pageSize
                        : (int)Math.Ceiling((double)pageSize / numberPartitions))
                    .ToArray();
                var continuationTokens = new JObject[numberPartitions];

                var tasks = new Task<ClientEvent>[numberPartitions];
                for (uint partitionId = 0; partitionId < numberPartitions; partitionId++)
                {
                    tasks[partitionId] = this.PerformRequestWithTimeoutAndCancellation(
                            cancellationToken,
                            new InstanceQueryReceived()
                            {
                                PartitionId = partitionId,
                                ClientId = this.ClientId,
                                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                                InstanceQuery = instanceQuery,
                                ContinuationToken = null,
                                PageSize = partitionId == 0   // query pattern is (100, 10, 10, ..., 10)
                                    ? pageSize
                                    : (int)Math.Ceiling((double)pageSize / numberPartitions),
                            },
                            false);
                }

                List<OrchestrationState> states = new List<OrchestrationState>();
                bool toBeContinued = false;
                JObject ftoken = null;
                var remainder = new StringBuilder();
                for (int i = 0; i < tasks.Length; i++)
                {
                    var response = (QueryResponseReceived)await tasks[i];
                    if (response.NonEmpty)
                    {
                        if (toBeContinued)
                        {
                            remainder.Append('1');
                        }
                        else
                        {
                            states.AddRange(response.OrchestrationStates);
                            if (states.Count >= pageSize)
                            {
                                toBeContinued = true;
                            }
                            if (response.ContinuationToken != null)
                            {
                                toBeContinued = true;
                                ftoken = JsonConvert.DeserializeObject<JObject>(response.ContinuationToken);
                            }
                        }
                    }
                    else
                    {
                        if (toBeContinued 
                            && (ftoken != null | remainder.Length > 0))
                        {
                            remainder.Append('0');
                        }
                    }            
                }

                string ctoken = null;
                if (ftoken != null || remainder.Length > 0)
                {
                    ctoken = JsonConvert.SerializeObject(new ContinuationToken()
                    {
                        FToken = ftoken,
                        Remainder = remainder.ToString(),
                    });
                }
 
                return new InstanceQueryResult()
                {
                    Instances = states,
                    ContinuationToken = ctoken,
                };
            }
            else
            {
                var token = (ContinuationToken)JsonConvert.DeserializeObject<ContinuationToken>(continuationTokenString);

                if (token.FToken == null)
                {
                    // we are starting the next partition in the remainder
                    Debug.Assert(token.Remainder.Length > 0 && token.Remainder[0] == '1');
                    token.Remainder = token.Remainder.Substring(1);
                }

                int position = (int) this.host.NumberPartitions - token.Remainder.Length - 1;

                var queryResponseReceived = (QueryResponseReceived) await this.PerformRequestWithTimeoutAndCancellation(
                        cancellationToken,
                        new InstanceQueryReceived()
                        {
                            PartitionId = (uint) position,
                            ClientId = this.ClientId,
                            RequestId = Interlocked.Increment(ref this.SequenceNumber),
                            TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                            InstanceQuery = instanceQuery,
                            ContinuationToken = token.FToken?.ToString(),
                            PageSize = pageSize
                        },
                        false);

                if (queryResponseReceived.ContinuationToken != null)
                {
                    token.FToken = JsonConvert.DeserializeObject<JObject>(queryResponseReceived.ContinuationToken);
                }
                else
                {
                    int next = token.Remainder.IndexOf('1');
                    if (next == -1)
                    {
                        token = null;
                    }
                    else
                    {
                        token.FToken = null;
                        token.Remainder = token.Remainder.Substring(next);
                    }
                }

                return new InstanceQueryResult()
                {
                    Instances = queryResponseReceived.OrchestrationStates,
                    ContinuationToken = token != null ? JsonConvert.SerializeObject(token) : null,
                };
            }
        }

        public async Task<(IList<OrchestrationState>,bool)> QueryParallelPagesAsync(
            InstanceQuery instanceQuery,
            JObject[] continuationTokens,
            (uint partitionId, int pageSize)[] pageSizes,
            CancellationToken cancellationToken)
        {
            var tasks = new Task<ClientEvent>[pageSizes.Length];
            for (int i = 0; i < pageSizes.Length; i++)
            {
                var partitionId = pageSizes[i].partitionId;
                tasks[i] = this.PerformRequestWithTimeoutAndCancellation(
                        cancellationToken,
                        new InstanceQueryReceived()
                        {
                            PartitionId = partitionId,
                            ClientId = this.ClientId,
                            RequestId = Interlocked.Increment(ref this.SequenceNumber),
                            TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                            InstanceQuery = instanceQuery,
                            ContinuationToken = continuationTokens[partitionId]?.ToString(),
                            PageSize = pageSizes[i].pageSize
                        },
                        false);
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            List<OrchestrationState> states = new List<OrchestrationState>();
            bool hasMore = false;
            for (int i = 0; i < pageSizes.Length; i++)
            {
                var partitionId = pageSizes[i].partitionId;
                var queryResponseReceived = (QueryResponseReceived)tasks[i].Result;
                foreach (var state in queryResponseReceived.OrchestrationStates)
                {
                    states.Add(state);
                }
                if (queryResponseReceived.ContinuationToken != null)
                {
                    hasMore = true;
                    continuationTokens[pageSizes[i].partitionId] =
                        queryResponseReceived.ContinuationToken != null ?
                        JObject.Parse(queryResponseReceived.ContinuationToken) : null;
                }
                else
                {
                    continuationTokens[pageSizes[i].partitionId] = null;
                }
            }
            return (states, hasMore);
        }



        public async Task<int> PurgeInstanceHistoryAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default)
        {
            IEnumerable<Task<ClientEvent>> launchQueries()
            {
                for (uint partitionId = 0; partitionId < this.host.NumberPartitions; ++partitionId)
                {
                    yield return this.PerformRequestWithTimeoutAndCancellation(
                        cancellationToken,
                        new PurgeRequestReceived()
                        {
                            PartitionId = partitionId,
                            ClientId = this.ClientId,
                            RequestId = Interlocked.Increment(ref this.SequenceNumber),
                            TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                            InstanceQuery = new InstanceQuery(
                                runtimeStatus?.ToArray(),
                                createdTimeFrom?.ToUniversalTime(),
                                createdTimeTo?.ToUniversalTime(),
                                null,
                                fetchInput: false)
                                { PrefetchHistory = true },
                        },
                        false);
                }
            }
            ClientEvent[] responses = await Task.WhenAll(launchQueries().ToList()).ConfigureAwait(false);
            return responses.Cast<PurgeResponseReceived>().Sum(response => response.NumberInstancesPurged);
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
