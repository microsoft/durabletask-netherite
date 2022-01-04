// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Scaling;

    [DataContract]
    class TimersState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, (DateTime due, TaskMessage message, string workItemId)> PendingTimers { get; private set; } 
            = new Dictionary<long, (DateTime, TaskMessage, string)>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Timers);

        public override string ToString()
        {
            return $"Timers ({this.PendingTimers.Count} pending) next={this.SequenceNumber:D6}";
        }

        public override void OnRecoveryCompleted(EffectTracker effects, RecoveryCompleted evt)
        {
            // restore the pending timers
            foreach (var kvp in this.PendingTimers)
            {
                this.Schedule(kvp.Key, kvp.Value.due, kvp.Value.message, kvp.Value.workItemId, effects);
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Timers = this.PendingTimers.Count;

            if (info.Timers > 0)
            {
                info.Wakeup = this.PendingTimers.Select(kvp => kvp.Value.Item1).Min();
            }
            else
            {
                info.Wakeup = null;
            }
        }

        void Schedule(long timerId, DateTime due, TaskMessage message, string originWorkItemId, EffectTracker effects)
        {
            TimerFired expirationEvent = new TimerFired()
            {
                PartitionId = this.Partition.PartitionId,
                TimerId = timerId,
                TaskMessage = message,
                OriginWorkItemId = originWorkItemId,
                Due = due,
            };

            effects.EventDetailTracer?.TraceEventProcessingDetail($"Scheduled {message} due at {expirationEvent.Due:o}, id={expirationEvent.EventIdString}");
            this.Partition.PendingTimers.Schedule(expirationEvent.Due, expirationEvent);
        }

        void AddTimer(TaskMessage taskMessage, string originWorkItemId, EffectTracker effects)
        {
            var timerId = this.SequenceNumber++;
            var due = GetDueTime(taskMessage);
            this.PendingTimers.Add(timerId, (due, taskMessage, originWorkItemId));

            if (!effects.IsReplaying)
            {
                this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, taskMessage, originWorkItemId, $"Timer@{due}");
                this.Schedule(timerId, due, taskMessage, originWorkItemId, effects);
            }
        }

        static DateTime GetDueTime(TaskMessage message)
        {
            if (message.Event is TimerFiredEvent timerFiredEvent)
            {
                return timerFiredEvent.FireAt;
            }
            else if (Entities.IsDelayedEntityMessage(message, out DateTime due))
            {
                return due;
            }
            else if (message.Event is ExecutionStartedEvent executionStartedEvent && executionStartedEvent.ScheduledStartTime.HasValue)
            {
                return executionStartedEvent.ScheduledStartTime.Value;
            }
            else
            {
                throw new ArgumentException(nameof(message), "unhandled event type");
            }
        }

        public override void Process(TimerFired evt, EffectTracker effects)
        {
            // removes the entry for the pending timer, and then adds it to the sessions queue
            this.PendingTimers.Remove(evt.TimerId);
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            // starts new timers as specified by the batch
            foreach (var taskMessage in evt.TimerMessages)
            {
                this.AddTimer(taskMessage, evt.EventIdString, effects);
            }
        }

        public override void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
            // starts new timers as specified by the batch
            foreach (var taskMessage in evt.DelayedTaskMessages)
            {
                this.AddTimer(taskMessage, evt.WorkItemId, effects);
            }
        }

        public override void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            this.AddTimer(creationRequestReceived.TaskMessage, creationRequestReceived.EventIdString, effects);
        }
    }
}
