// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

        public override void OnRecoveryCompleted()
        {
            // restore the pending timers
            foreach (var kvp in this.PendingTimers)
            {
                this.Schedule(kvp.Key, kvp.Value.due, kvp.Value.message, kvp.Value.workItemId);
            }
        }

        // how long before the scheduled time the ScalingMonitor should scale up from zero
        static readonly TimeSpan WakeupInAdvance = TimeSpan.FromSeconds(20);

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Timers = this.PendingTimers.Count;

            if (info.Timers > 0)
            {
                info.Wakeup = this.PendingTimers.Select(kvp => kvp.Value.Item1).Min() - WakeupInAdvance;
            }
            else
            {
                info.Wakeup = null;
            }
        }

        void Schedule(long timerId, DateTime due, TaskMessage message, string originWorkItemId)
        {
            TimerFired expirationEvent = new TimerFired()
            {
                PartitionId = this.Partition.PartitionId,
                TimerId = timerId,
                TaskMessage = message,
                OriginWorkItemId = originWorkItemId,
                Due = due,
            };

            this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Scheduled {message} due at {expirationEvent.Due:o}, id={expirationEvent.EventIdString}");
            this.Partition.PendingTimers.Schedule(expirationEvent.Due, expirationEvent);
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

        public void Process(TimerFired evt, EffectTracker effects)
        {
            // removes the entry for the pending timer, and then adds it to the sessions queue
            this.PendingTimers.Remove(evt.TimerId);
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // starts new timers as specified by the batch
            foreach (var t in evt.TimerMessages)
            {
                var timerId = this.SequenceNumber++;
                var due = GetDueTime(t);
                string workItemId = evt.EventIdString;
                this.PendingTimers.Add(timerId, (due, t, workItemId));

                if (!effects.IsReplaying)
                {
                    this.Schedule(timerId, due, t, workItemId);
                }
            }
        }

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
            // starts new timers as specified by the batch
            foreach (var t in evt.DelayedTaskMessages)
            {
                var timerId = this.SequenceNumber++;
                var due = GetDueTime(t);
                this.PendingTimers.Add(timerId, (due, t, evt.WorkItemId));

                if (!effects.IsReplaying)
                {
                    this.Schedule(timerId, due, t, evt.WorkItemId);
                }
            }
        }

        public void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            // starts a new timer for the execution started event
            var timerId = this.SequenceNumber++;
            var due = GetDueTime(creationRequestReceived.TaskMessage);
            this.PendingTimers.Add(timerId, (due, creationRequestReceived.TaskMessage, creationRequestReceived.EventIdString));

            if (!effects.IsReplaying)
            {
                this.Schedule(timerId, due, creationRequestReceived.TaskMessage, creationRequestReceived.WorkItemId);
            }
        }
    }
}
