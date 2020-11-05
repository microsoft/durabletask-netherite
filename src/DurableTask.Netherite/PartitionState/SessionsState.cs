// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
            public bool ForceNewExecution { get; set; }

            [IgnoreDataMember]
            public OrchestrationMessageBatch CurrentBatch { get; set; }
        }

        [DataMember]
        public Dictionary<string, BatchProcessed> StepsAwaitingPersistence { get; private set; } = new Dictionary<string, BatchProcessed>();

        [IgnoreDataMember]
        public HashSet<OrchestrationMessageBatch> PendingMessageBatches { get; set; } = new HashSet<OrchestrationMessageBatch>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Sessions);

        public static string GetWorkItemId(uint partition, long session, long position) => $"{partition:D2}S{session}P{position}";


        public override void OnRecoveryCompleted()
        {
            // start work items for all sessions
            foreach (var kvp in this.Sessions)
            {
                new OrchestrationMessageBatch(kvp.Key, kvp.Value, this.Partition);
            }
            
            // handle all steps that were awaiting persistence
            foreach(var kvp in this.StepsAwaitingPersistence)
            {
                this.ConfirmDurable(kvp.Value);
            }
        }        

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.WorkItems += this.Sessions.Count;
            double now = this.Partition.CurrentTimeMs;
            info.WorkItemLatencyMs = (long) this.Sessions.Values.Select(session => session.CurrentBatch?.WaitTimeMs(now) ?? 0).DefaultIfEmpty().Max();
        }

        public override string ToString()
        {
            return $"Sessions ({this.Sessions.Count} pending) next={this.SequenceNumber:D6}";
        }

        string GetSessionPosition(Session session) => $"{this.Partition.PartitionId:D2}S{session.SessionId}P{session.BatchStartPosition + session.Batch.Count}";
      

        void AddMessageToSession(TaskMessage message, string originWorkItemId, bool isReplaying)
        {
            string instanceId = message.OrchestrationInstance.InstanceId;
            bool forceNewExecution = message.Event is ExecutionStartedEvent;
            this.Partition.Assert(!string.IsNullOrEmpty(originWorkItemId));

            if (this.Sessions.TryGetValue(instanceId, out var session) && !forceNewExecution)
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages when it completes.
                this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));

                session.Batch.Add((message, originWorkItemId));
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = this.SequenceNumber++,
                    Batch = new List<(TaskMessage,string)>(),
                    BatchStartPosition = 0,
                    ForceNewExecution = forceNewExecution,
                };

                this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));
                session.Batch.Add((message, originWorkItemId));

                if (!isReplaying) // during replay, we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session, this.Partition);
                }
            }
        }

        void AddMessagesToSession(string instanceId, string originWorkItemId, IEnumerable<TaskMessage> messages, bool isReplaying)
        {
            this.Partition.Assert(!string.IsNullOrEmpty(originWorkItemId));
            int? forceNewExecution = FindLastExecutionStartedEvent(messages);

            if (this.Sessions.TryGetValue(instanceId, out var session) && forceNewExecution == null)
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages 
                // when the previous work item completes.
                foreach(var message in messages)
                {
                    this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));                  
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
                    foreach (var taskMessage in messages.Take(forceNewExecution.Value))
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageDiscarded(this.Partition.PartitionId, taskMessage, originWorkItemId, "message bound for an instance that was replaced");
                    }

                    messages = messages.Skip(forceNewExecution.Value);
                }
          
                // Create a new session
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = this.SequenceNumber++,
                    Batch = new List<(TaskMessage,string)>(),
                    BatchStartPosition = 0,
                    ForceNewExecution = forceNewExecution.HasValue,
                };

                foreach (var message in messages)
                {
                    this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, message, originWorkItemId, this.GetSessionPosition(session));
                    session.Batch.Add((message, originWorkItemId));
                }

                if (!isReplaying) // we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session, this.Partition);
                }
            }
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

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
             // queues task message (from another partition) in a new or existing session
           foreach (var group in evt.TaskMessages
                .GroupBy(tm => tm.OrchestrationInstance.InstanceId))
            {
                this.AddMessagesToSession(group.Key, evt.WorkItemId, group, effects.IsReplaying);
            }
        }

        public void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            // queues task message (from another partition) in a new or existing session
            this.AddMessageToSession(evt.Result, evt.WorkItemId, effects.IsReplaying);
        }

        public void Process(ClientTaskMessagesReceived evt, EffectTracker effects)
        {
            // queues task message (from a client) in a new or existing session
            var instanceId = evt.TaskMessages[0].OrchestrationInstance.InstanceId;
            this.AddMessagesToSession(instanceId, evt.WorkItemId, evt.TaskMessages, effects.IsReplaying);
        }

        public void Process(TimerFired timerFired, EffectTracker effects)
        {
            // queues a timer fired message in a session
            this.AddMessageToSession(timerFired.TaskMessage, timerFired.OriginWorkItemId, effects.IsReplaying);
        }

        public void Process(ActivityCompleted activityCompleted, EffectTracker effects)
        {
            // queues an activity-completed message in a session
            this.AddMessageToSession(activityCompleted.Response, activityCompleted.WorkItemId, effects.IsReplaying);
        }

        public void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            // queues the execution started message
            this.AddMessageToSession(creationRequestReceived.TaskMessage, creationRequestReceived.WorkItemId, effects.IsReplaying);
        }

        public void Process(DeletionRequestReceived deletionRequestReceived, EffectTracker effects)
        {
            // removing the session means that all pending messages will be deleted also.
            this.Sessions.Remove(deletionRequestReceived.InstanceId);
        }

        public void Process(PurgeBatchIssued purgeBatchIssued, EffectTracker effects)
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
            evtCopy.IsPersisted = true;
            this.Partition.SubmitInternalEvent(evtCopy);
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // if speculation is disabled, 
            if (effects.Partition.Settings.PersistStepsFirst)
            {
                if (!evt.IsPersisted)
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
                this.Partition.WorkItemTraceHelper.TraceWorkItemDiscarded(this.Partition.PartitionId, WorkItemTraceHelper.WorkItemType.Orchestration, evt.WorkItemId, evt.InstanceId);    
                return;
            };

            if (evt.ActivityMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Activities);
            }

            if (evt.TimerMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Timers);
            }

            if (evt.RemoteMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Outbox);
            }

            // deliver orchestrator messages destined for this partition directly to the relevant session(s)
            if (evt.LocalMessages?.Count > 0)
            {
                foreach (var group in evt.LocalMessages.GroupBy(tm => tm.OrchestrationInstance.InstanceId))
                {
                    this.AddMessagesToSession(group.Key, evt.WorkItemId, group, effects.IsReplaying);
                }
            }

            if (evt.State != null)
            {
                effects.Add(TrackedObjectKey.Instance(evt.InstanceId));
                effects.Add(TrackedObjectKey.History(evt.InstanceId));
            }

            // remove processed messages from this batch
            effects.Partition.Assert(session != null);
            effects.Partition.Assert(session.SessionId == evt.SessionId);
            effects.Partition.Assert(session.BatchStartPosition == evt.BatchStartPosition);
            session.Batch.RemoveRange(0, evt.BatchLength);
            session.BatchStartPosition += evt.BatchLength;

            this.StartNewBatchIfNeeded(session, effects, evt.InstanceId, effects.IsReplaying);
        }

        void StartNewBatchIfNeeded(Session session, EffectTracker effects, string instanceId, bool inRecovery)
        {
            if (session.Batch.Count == 0)
            {
                // no more pending messages for this instance, so we delete the session.
                this.Sessions.Remove(instanceId);
            }
            else
            {
                if (!inRecovery) // we don't start work items until end of recovery
                {
                    // there are more messages. Start another work item.
                    new OrchestrationMessageBatch(instanceId, session, this.Partition);
                }
            }
        }
    }
}
