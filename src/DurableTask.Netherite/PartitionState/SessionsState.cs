﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Scaling;

    [DataContract]
    class SessionsState : TrackedObject, TransportAbstraction.IDurabilityListener
    {
        [DataMember]
        public Dictionary<string, Session> Sessions { get; private set; } = new Dictionary<string, Session>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [DataContract]
        internal class Session
        {
            [DataMember]
            public long SessionId { get; set; }

            [DataMember]
            public long BatchStartPosition { get; set; }

            [DataMember]
            public List<(TaskMessage message, string originWorkItemId)> Batch { get; set; }

            [DataMember]
            public int DequeueCount { get; set; }

            [DataMember]
            public bool ForceNewExecution { get; set; }

            [IgnoreDataMember]
            public OrchestrationMessageBatch CurrentBatch { get; set; }
        }

        [DataMember]
        public Dictionary<string, BatchProcessed> StepsAwaitingPersistence { get; private set; } = new Dictionary<string, BatchProcessed>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Sessions);

        public static string GetWorkItemId(uint partition, long session, long position) => $"{partition:D2}S{session}P{position}";

        public override void Process(RecoveryCompleted evt, EffectTracker effects)
        {
            // restart work items for all sessions
            foreach (var kvp in this.Sessions)
            {
                kvp.Value.DequeueCount++;

                if (!effects.IsReplaying) // during replay, we don't start work items until end of recovery
                {
                    // submit a message batch for processing
                    new OrchestrationMessageBatch(kvp.Key, kvp.Value, this.Partition, evt, waitForPersistence: true);
                }
            }

            if (!effects.IsReplaying)
            {
                foreach (var kvp in this.StepsAwaitingPersistence)
                {
                    this.ConfirmDurable(kvp.Value);
                }
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            if (this.Sessions.Count > 0)
            {
                info.WorkItems += this.Sessions.Count;

                double now = this.Partition.CurrentTimeMs;
                var maxLatencyInQueue = (long)this.Sessions.Values.Select(session => session.CurrentBatch?.WaitTimeMs(now) ?? 0).DefaultIfEmpty().Max();

                if (maxLatencyInQueue < 100)
                {
                    info.MarkActive();
                }
                else if (maxLatencyInQueue < 1000)
                {
                    info.MarkMediumLatency();
                }
                else
                {
                    info.MarkHighLatency();
                }
            }
        }

        public override string ToString()
        {
            return $"Sessions Count={this.Sessions.Count} next={this.SequenceNumber:D6}";
        }

        string GetSessionId(Session session) => $"{this.Partition.PartitionId:D2}S{session.SessionId}";
        string GetSessionPosition(Session session) => $"{this.Partition.PartitionId:D2}S{session.SessionId}P{session.BatchStartPosition + session.Batch.Count}";
      

        void AddMessageToSession(TaskMessage message, string originWorkItemId, bool isReplaying, PartitionUpdateEvent filingEvent)
        {
            string instanceId = message.OrchestrationInstance.InstanceId;
            bool forceNewExecution = message.Event is ExecutionStartedEvent;
            this.Partition.Assert(!string.IsNullOrEmpty(originWorkItemId), "null originWorkItem");

            if (this.Sessions.TryGetValue(instanceId, out var session) && !forceNewExecution)
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages when it completes.

                if (!isReplaying)
                {
                    this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));
                }

                session.Batch.Add((message, originWorkItemId));
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = this.SequenceNumber++,
                    Batch = new List<(TaskMessage,string)>(),
                    BatchStartPosition = 0,
                    DequeueCount = 1,
                    ForceNewExecution = forceNewExecution,
                };

                if (!isReplaying)
                {
                    this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));
                }

                session.Batch.Add((message, originWorkItemId));

                if (!isReplaying) // during replay, we don't start work items until end of recovery
                {
                    // submit a message batch for processing
                    new OrchestrationMessageBatch(instanceId, session, this.Partition, filingEvent, waitForPersistence: false);
                }
            }
        }

        Session AddMessagesToSession(string instanceId, string originWorkItemId, IEnumerable<TaskMessage> messages, bool isReplaying, PartitionUpdateEvent filingEvent)
        {
            this.Partition.Assert(!string.IsNullOrEmpty(originWorkItemId), "null originWorkItem");
            int? forceNewExecution = FindLastExecutionStartedEvent(messages);

            if (this.Sessions.TryGetValue(instanceId, out var session) && forceNewExecution == null)
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages 
                // when the previous work item completes.
                foreach (var message in messages)
                {
                    if (!isReplaying)
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));
                    }

                    session.Batch.Add((message, originWorkItemId));
                }
            }
            else
            {
                if (forceNewExecution.HasValue)
                {
                    // the new instance replaces whatever state the old instance was in
                    // since our transport is exactly once and in order, we do not save the old messages
                    // but "consider them delivered" to the old instance which is then replaced
                    if (!isReplaying)
                    {
                        foreach (var taskMessage in messages.Take(forceNewExecution.Value))
                        {
                            this.Partition.WorkItemTraceHelper.TraceTaskMessageDiscarded(this.Partition.PartitionId, taskMessage, originWorkItemId, "message bound for an instance that was replaced");
                        }
                    }
                    messages = messages.Skip(forceNewExecution.Value);
                }

                // Create a new session
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = this.SequenceNumber++,
                    Batch = new List<(TaskMessage, string)>(),
                    BatchStartPosition = 0,
                    DequeueCount = 1,
                    ForceNewExecution = forceNewExecution.HasValue,
                };

                foreach (var message in messages)
                {
                    if (!isReplaying)
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));
                    }

                    session.Batch.Add((message, originWorkItemId));
                }

                if (!isReplaying) // we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session, this.Partition, filingEvent, waitForPersistence: false);
                }
            }

            return session;
        }

        static int? FindLastExecutionStartedEvent(IEnumerable<TaskMessage> messages)
        {
            int? lastOccurence = null;
            int position = 0;
            foreach (TaskMessage taskMessage in messages)
            {
                if (taskMessage.Event is ExecutionStartedEvent)
                {
                    lastOccurence = position;
                }
                position++;
            }
            return lastOccurence;
        }

        public override void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
             // queues task message (from another partition) in a new or existing session
           foreach (var group in evt.TaskMessages
                .GroupBy(tm => tm.OrchestrationInstance.InstanceId))
            {
                this.AddMessagesToSession(group.Key, evt.WorkItemId, group, effects.IsReplaying, evt);
            }
        }

        public override void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            // queues task message (from another partition) in a new or existing session
            this.AddMessageToSession(evt.Result, evt.WorkItemId, effects.IsReplaying, evt);
        }

        public override void Process(ClientTaskMessagesReceived evt, EffectTracker effects)
        {
            // queues task message (from a client) in a new or existing session
            var instanceId = evt.TaskMessages[0].OrchestrationInstance.InstanceId;
            this.AddMessagesToSession(instanceId, evt.WorkItemId, evt.TaskMessages, effects.IsReplaying, evt);
        }

        public override void Process(TimerFired timerFired, EffectTracker effects)
        {
            // queues a timer fired message in a session
            this.AddMessageToSession(timerFired.TaskMessage, timerFired.OriginWorkItemId, effects.IsReplaying, timerFired);
        }

        public override void Process(ActivityCompleted activityCompleted, EffectTracker effects)
        {
            // queues an activity-completed message in a session
            this.AddMessageToSession(activityCompleted.Response, activityCompleted.WorkItemId, effects.IsReplaying, activityCompleted);
        }

        public override void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            // queues the execution started message
            this.AddMessageToSession(creationRequestReceived.TaskMessage, creationRequestReceived.WorkItemId, effects.IsReplaying, creationRequestReceived);
        }

        public override void Process(DeletionRequestReceived deletionRequestReceived, EffectTracker effects)
        {
            // removing the session means that all pending messages will be deleted also.
            this.Sessions.Remove(deletionRequestReceived.InstanceId);
        }

        public override void Process(PurgeBatchIssued purgeBatchIssued, EffectTracker effects)
        {
            foreach (string instanceId in purgeBatchIssued.Purged)
            {
                // removing the session means that all pending messages will be deleted also.
                this.Sessions.Remove(instanceId);
            }
        }

        public void ConfirmDurable(Event evt)
        {
            var evtCopy = (BatchProcessed) ((BatchProcessed) evt).Clone();
            evtCopy.PersistFirst = BatchProcessed.PersistFirstStatus.Done;
            this.Partition.SubmitEvent(evtCopy);
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            // if speculation is disabled, 
            if (evt.PersistFirst != BatchProcessed.PersistFirstStatus.NotRequired)
            {
                if (evt.PersistFirst == BatchProcessed.PersistFirstStatus.Required)
                {
                    // we do not process this event right away
                    // but persist it first, and then submit it again, before processing it.
                    this.StepsAwaitingPersistence.Add(evt.WorkItemId, evt);
                    DurabilityListeners.Register(evt, this);
                    return;
                }
                else
                {
                    this.StepsAwaitingPersistence.Remove(evt.WorkItemId);
                }
            }

            // updates the session and other state

            // our instance may already be obsolete if it has been forcefully replaced.
            // This can manifest as the instance having disappeared, or as the current instance having
            // a different session id
            if (!this.Sessions.TryGetValue(evt.InstanceId, out var session) || session.SessionId != evt.SessionId)
            {
                this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"discarded evtsession={evt.SessionId} actualsession={(session != null ? this.GetSessionPosition(session).ToString() : "none")}");

                if (!effects.IsReplaying)
                {
                    this.Partition.WorkItemTraceHelper.TraceWorkItemDiscarded(
                        this.Partition.PartitionId,
                        DurableTask.Core.Common.Entities.IsEntityInstance(evt.InstanceId) ? WorkItemTraceHelper.WorkItemType.Entity : WorkItemTraceHelper.WorkItemType.Orchestration,
                        evt.WorkItemId, evt.InstanceId,
                        session != null ? this.GetSessionPosition(session) : null,
                        "session was replaced");
                }
                return;
            };

            // detect loopback messages, to guarantee that they act as a persistence barrier
            bool containsLoopbackMessages = false;

            if (!evt.NotExecutable)
            {

                if (evt.ActivityMessages?.Count > 0)
                {
                    effects.Add(TrackedObjectKey.Activities);
                }

                if (evt.TimerMessages?.Count > 0)
                {
                    effects.Add(TrackedObjectKey.Timers);
                }

                if (evt.RemoteMessages?.Count > 0 || WaitRequestReceived.SatisfiesWaitCondition(evt.OrchestrationStatus))
                {
                    effects.Add(TrackedObjectKey.Outbox);
                }

                // deliver orchestrator messages destined for this partition directly to the relevant session(s)
                if (evt.LocalMessages?.Count > 0)
                {
                    foreach (var group in evt.LocalMessages.GroupBy(tm => tm.OrchestrationInstance.InstanceId))
                    {
                        var targetSession = this.AddMessagesToSession(group.Key, evt.WorkItemId, group, effects.IsReplaying, evt);

                        if (targetSession == session)
                        {
                            containsLoopbackMessages = true;
                        }
                    }
                }

                effects.Add(TrackedObjectKey.Instance(evt.InstanceId));
                effects.Add(TrackedObjectKey.History(evt.InstanceId));
            }

            // remove processed messages from this batch
            effects.Assert(session != null, "null session in SessionsState.Process(BatchProcessed)");
            effects.Assert(session.SessionId == evt.SessionId, "wrong session id in SessionsState.Process(BatchProcessed)");
            effects.Assert(session.BatchStartPosition == evt.BatchStartPosition, "wrong start position in SessionsState.Process(BatchProcessed)");
            session.Batch.RemoveRange(0, evt.BatchLength);
            session.BatchStartPosition += evt.BatchLength;
            session.DequeueCount = 1;

            // start a new batch if needed      
            if (session.Batch.Count == 0)
            {
                // no more pending messages for this instance, so we delete the session.
                this.Sessions.Remove(evt.InstanceId);
            }
            else
            {
                // there are more messages to process

                if (!effects.IsReplaying) // we don't start work items until end of recovery
                {
                    // submit a message batch for processing
                    new OrchestrationMessageBatch(evt.InstanceId, session, this.Partition, evt, waitForPersistence: containsLoopbackMessages);
                }
            }
        }
    }
}
