// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;

    class Client : TransportAbstraction.IClient
    {
        readonly NetheriteOrchestrationService host;
        readonly CancellationToken shutdownToken;
        readonly ClientTraceHelper traceHelper;
        readonly string account;
        readonly Guid taskHubGuid;
        readonly WorkItemTraceHelper workItemTraceHelper;

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

            if (doneWhenSent)
            {
                DurabilityListeners.Register((Event)request, pendingRequest);
            }

            this.Send(partitionEvent);

            return pendingRequest.Task;
        }

        internal class PendingRequest : TransportAbstraction.IDurabilityOrExceptionListener
        {
            readonly long requestId;
            readonly EventId eventId;
            readonly uint partitionId;
            readonly Client client;
            readonly (DateTime due, int id) timeoutKey;
            readonly TaskCompletionSource<ClientEvent> continuation;

            static readonly TimeoutException timeoutException = new TimeoutException("Client request timed out.");

            public Task<ClientEvent> Task => this.continuation.Task;
            public (DateTime, int) TimeoutKey => this.timeoutKey;

            public string RequestId => $"{Client.GetShortId(this.client.ClientId)}-{this.requestId}"; // matches EventId

            public PendingRequest(long requestId, EventId eventId, uint partitionId, Client client, DateTime due, int timeoutId)
            {
                this.requestId = requestId;
                this.partitionId = partitionId;
                this.eventId = eventId;
                this.client = client;
                this.timeoutKey = (due, timeoutId);
                this.continuation = new TaskCompletionSource<ClientEvent>(TaskContinuationOptions.ExecuteSynchronously);
            }

            public void Respond(ClientEvent evt)
            {
                this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                this.continuation.TrySetResult(evt);
            }

            void TransportAbstraction.IDurabilityListener.ConfirmDurable(Event evt)
            {
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                {
                    this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                    this.continuation.TrySetResult(null); // task finishes when the send has been confirmed, no result is returned
                }
            }

            void TransportAbstraction.IDurabilityOrExceptionListener.ReportException(Event evt, Exception e)
            {
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                {
                    this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                    this.continuation.TrySetException(e); // task finishes with exception
                }
            }

            public void TryTimeout()
            {
                this.client.traceHelper.TraceTimerProgress($"firing ({this.timeoutKey.due:o},{this.timeoutKey.id})");
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var pendingRequest))
                {
                    this.client.traceHelper.TraceRequestTimeout(pendingRequest.eventId, pendingRequest.partitionId);
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

            this.workItemTraceHelper.TraceWorkItemCompleted(
                partitionId,
                WorkItemTraceHelper.WorkItemType.Client,
                request.WorkItemId,
                creationMessage.OrchestrationInstance.InstanceId,
                WorkItemTraceHelper.ClientStatus.Create,
                WorkItemTraceHelper.FormatMessageIdList(request.TracedTaskMessages));

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
            foreach (var group in messages.GroupBy((message) => message.OrchestrationInstance.InstanceId))
            {
                this.workItemTraceHelper.TraceWorkItemCompleted(
                    partitionId,
                    WorkItemTraceHelper.WorkItemType.Client,
                    request.WorkItemId,
                    group.Key,
                    WorkItemTraceHelper.ClientStatus.Send,
                    WorkItemTraceHelper.FormatMessageIdList(group.Select((message) => (message, request.WorkItemId))));
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
            => this.RunPartitionQueries<InstanceQueryReceived, QueryResponseReceived, IList<OrchestrationState>>(
                partitionId => new InstanceQueryReceived() {
                    PartitionId = partitionId,
                    ClientId = this.ClientId,
                    RequestId = Interlocked.Increment(ref this.SequenceNumber),
                    TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                    InstanceQuery = new InstanceQuery(),
                },
                (IEnumerable<QueryResponseReceived> responses) => responses.SelectMany(response => response.OrchestrationStates).ToList(),
                cancellationToken);

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo,
                    IEnumerable<OrchestrationStatus> runtimeStatus, string instanceIdPrefix, CancellationToken cancellationToken = default)
            => this.RunPartitionQueries<InstanceQueryReceived,QueryResponseReceived,IList<OrchestrationState>>(
                partitionId => new InstanceQueryReceived() {
                    PartitionId = partitionId,
                    ClientId = this.ClientId,
                    RequestId = Interlocked.Increment(ref this.SequenceNumber),
                    TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                    InstanceQuery = new InstanceQuery(
                        runtimeStatus?.ToArray(), 
                        createdTimeFrom?.ToUniversalTime(),
                        createdTimeTo?.ToUniversalTime(),
                        instanceIdPrefix,
                        fetchInput: true),
                },
                (IEnumerable<QueryResponseReceived> responses) => responses.SelectMany(response => response.OrchestrationStates).ToList(),
                cancellationToken);

        public Task<int> PurgeInstanceHistoryAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default)
            => this.RunPartitionQueries(
                partitionId => new PurgeRequestReceived()
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
                (IEnumerable<PurgeResponseReceived> responses) => responses.Sum(response => response.NumberInstancesPurged),
                cancellationToken);

        public Task<InstanceQueryResult> QueryOrchestrationStatesAsync(InstanceQuery instanceQuery, int pageSize, string continuationToken, CancellationToken cancellationToken)
            => this.RunPartitionQueries(
                partitionId => new InstanceQueryReceived()
                {
                    PartitionId = partitionId,
                    ClientId = this.ClientId,
                    RequestId = Interlocked.Increment(ref this.SequenceNumber),
                    TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                    InstanceQuery = instanceQuery,
                },
                (IEnumerable<QueryResponseReceived> responses) => new InstanceQueryResult()
                {
                    Instances = responses.SelectMany(response => response.OrchestrationStates),
                    ContinuationToken = null,
                },
                cancellationToken);

        async Task<TResult> RunPartitionQueries<TRequest,TResponse,TResult>(
            Func<uint, TRequest> requestCreator, 
            Func<IEnumerable<TResponse>,TResult> responseAggregator,
            CancellationToken cancellationToken)
            where TRequest: IClientRequestEvent
        {
            IEnumerable<Task<ClientEvent>> launchQueries()
            {
                for (uint partitionId = 0; partitionId < this.host.NumberPartitions; ++partitionId)
                {
                    yield return this.PerformRequestWithTimeoutAndCancellation(cancellationToken, requestCreator(partitionId), false);
                }
            }

            return responseAggregator((await Task.WhenAll(launchQueries()).ConfigureAwait(false)).Cast<TResponse>());
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
