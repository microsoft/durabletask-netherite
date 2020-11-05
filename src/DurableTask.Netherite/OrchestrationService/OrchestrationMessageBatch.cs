// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;

    class OrchestrationMessageBatch : InternalReadEvent
    {
        public string InstanceId;
        public long SessionId;
        public long BatchStartPosition;
        public int BatchLength;
        public bool ForceNewExecution;
        public List<TaskMessage> MessagesToProcess = new List<TaskMessage>();
        public List<string> MessagesToProcessOrigin = new List<string>();
        public string WorkItemId;
        public double? WaitingSince; // measures time waiting to execute

        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);

        public OrchestrationMessageBatch(string instanceId, SessionsState.Session session, Partition partition)
        {
            this.InstanceId = instanceId;
            this.SessionId = session.SessionId;
            this.BatchStartPosition = session.BatchStartPosition;
            this.BatchLength = session.Batch.Count;
            this.ForceNewExecution = session.ForceNewExecution && session.BatchStartPosition == 0;
            foreach ((TaskMessage message, string originWorkItemId) in session.Batch) 
            {
                this.MessagesToProcess.Add(message);
                this.MessagesToProcessOrigin.Add(originWorkItemId);
            }
            this.WorkItemId = SessionsState.GetWorkItemId(partition.PartitionId, this.SessionId, this.BatchStartPosition);
            this.WaitingSince = partition.CurrentTimeMs;

            session.CurrentBatch = this; 

            partition.EventDetailTracer?.TraceEventProcessingDetail($"OrchestrationMessageBatch is prefetching instance={this.InstanceId} batch={this.WorkItemId}");

            // continue when we have the history state loaded, which gives us the latest state and/or cursor
            partition.SubmitInternalEvent(this);
        }

        public IEnumerable<(TaskMessage, string)> TracedMessages
        {
            get 
            {
                // Autostart may have added an extra first ExecutionStartedEvent message which we want to skip. 
                var messagesToTrace = this.MessagesToProcess.Skip(this.MessagesToProcess.Count - this.MessagesToProcessOrigin.Count);
                return Enumerable.Zip(
                    messagesToTrace,
                    this.MessagesToProcessOrigin, 
                    (a, b) => (a, b));
            }
        }

        public override TrackedObjectKey ReadTarget => TrackedObjectKey.History(this.InstanceId);

        public override TrackedObjectKey? Prefetch => TrackedObjectKey.Instance(this.InstanceId);

        public double WaitTimeMs(double now) => (now - this.WaitingSince) ?? 0;

        public override void OnReadComplete(TrackedObject s, Partition partition)
        {
            var historyState = (HistoryState)s;
            OrchestrationWorkItem workItem;

            if (this.ForceNewExecution || historyState == null)
            {
                // we either have no previous instance, or want to replace the previous instance
                workItem = new OrchestrationWorkItem(partition, this, previousHistory: null);
                workItem.Type = OrchestrationWorkItem.ExecutionType.Fresh;
            }
            else if (historyState.CachedOrchestrationWorkItem != null)
            {
                // we have a cached cursor and can resume where we left off
                workItem = historyState.CachedOrchestrationWorkItem;
                workItem.SetNextMessageBatch(this);
                workItem.Type = OrchestrationWorkItem.ExecutionType.ContinueFromCursor;

                // sanity check: it appears cursor is sometimes corrupted, in that case, construct fresh
                // TODO investigate reasons and fix root cause
                if (workItem.OrchestrationRuntimeState?.Events?.Count != historyState.History?.Count
                    || workItem.OrchestrationRuntimeState?.OrchestrationInstance?.ExecutionId != historyState.ExecutionId)
                {
                    partition.EventTraceHelper.TraceEventProcessingWarning($"Fixing bad workitem cache instance={this.InstanceId} batch={this.WorkItemId} expected_size={historyState.History?.Count} actual_size={workItem.OrchestrationRuntimeState?.Events?.Count} expected_executionid={historyState.ExecutionId} actual_executionid={workItem.OrchestrationRuntimeState?.OrchestrationInstance?.ExecutionId}");

                    // we create a new work item and rehydrate the instance from its history
                    workItem = new OrchestrationWorkItem(partition, this, previousHistory: historyState.History);
                    workItem.Type = OrchestrationWorkItem.ExecutionType.ContinueFromHistory;
                }
            }
            else
            {
                // we have to rehydrate the instance from its history
                workItem = new OrchestrationWorkItem(partition, this, previousHistory: historyState.History);
                workItem.Type = OrchestrationWorkItem.ExecutionType.ContinueFromHistory;
            }

            if (!this.IsExecutableInstance(workItem, out var reason))
            {
                // this instance cannot be executed, so we are discarding the messages
                for (int i = 0; i < workItem.MessageBatch.MessagesToProcess.Count; i++)
                {
                    partition.WorkItemTraceHelper.TraceTaskMessageDiscarded(
                        this.PartitionId,
                        workItem.MessageBatch.MessagesToProcess[i],
                        workItem.MessageBatch.MessagesToProcessOrigin[i],
                        reason);
                }

                // we issue a batch processed event which will remove the messages without updating the instance state
                partition.SubmitInternalEvent(new BatchProcessed()
                {
                    PartitionId = partition.PartitionId,
                    SessionId = this.SessionId,
                    InstanceId = this.InstanceId,
                    BatchStartPosition = this.BatchStartPosition,
                    BatchLength = this.BatchLength,
                    NewEvents = null,
                    State = null,
                    WorkItemForReuse = null,
                    ActivityMessages = null,
                    LocalMessages = null,
                    RemoteMessages = null,
                    TimerMessages = null,
                    Timestamp = DateTime.UtcNow,
                });
            }
            else
            {
                // the work item is ready to process - put it into the OrchestrationWorkItemQueue
                partition.EnqueueOrchestrationWorkItem(workItem);
            }
        }

        bool IsExecutableInstance(TaskOrchestrationWorkItem workItem, out string message)
        {
            if (workItem.OrchestrationRuntimeState.ExecutionStartedEvent == null
                && !this.MessagesToProcess.Any(msg => msg.Event is ExecutionStartedEvent))
            {
                if (DurableTask.Core.Common.Entities.AutoStart(this.InstanceId, this.MessagesToProcess))
                {
                    message = default;
                    return true;
                }
                else
                {
                    message = workItem.NewMessages.Count == 0 ? "No such instance" : "Instance is corrupted";
                    return false;
                }
            }

            if (workItem.OrchestrationRuntimeState.ExecutionStartedEvent != null &&
                workItem.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                workItem.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                message = $"Instance is {workItem.OrchestrationRuntimeState.OrchestrationStatus}";
                return false;
            }

            message = null;
            return true;
        }
    }
}
