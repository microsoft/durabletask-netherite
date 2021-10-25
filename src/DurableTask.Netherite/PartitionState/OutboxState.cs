// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Netherite.ScalingLogic;

    [DataContract]
    class OutboxState : TrackedObject, TransportAbstraction.IDurabilityListener
    {
        [DataMember]
        public SortedDictionary<long, Batch> Outbox { get; private set; } = new SortedDictionary<long, Batch>();

        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        public override void OnRecoveryCompleted()
        {
            // resend all pending
            foreach (var kvp in this.Outbox)
            {
                // recover non-persisted fields
                kvp.Value.Position = kvp.Key;
                kvp.Value.Partition = this.Partition;

                // resend (anything we have recovered is of course persisted)
                this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Resent {kvp.Key:D10} ({kvp.Value} messages)");
                this.Send(kvp.Value);
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Outbox = this.Outbox.Count;
        }

        public override string ToString()
        {
            return $"Outbox ({this.Outbox.Count} pending)";
        }

        void SendBatchOnceEventIsPersisted(PartitionUpdateEvent evt, EffectTracker effects, Batch batch)
        {
            // put the messages in the outbox where they are kept until actually sent
            var commitPosition = evt.NextCommitLogPosition;

            this.Outbox[commitPosition] = batch;
            batch.Position = commitPosition;
            batch.Partition = this.Partition;

            if (!effects.IsReplaying)
            {
                if (evt is BatchProcessed batchProcessedEvt 
                    && batchProcessedEvt.PersistFirst == BatchProcessed.PersistFirstStatus.Done)
                {
                    // in this special case the event is actually already persisted so we can send right away
                    this.Send(batch);
                    return;
                }

                // register for a durability notification, at which point we will send the batch
                evt.OutboxBatch = batch;
                batch.ProcessedTimestamp = this.Partition.CurrentTimeMs;
                DurabilityListeners.Register(evt, this);
            }
        }

        public void ConfirmDurable(Event evt)
        {
            var partitionUpdateEvent = ((PartitionUpdateEvent)evt);
            this.Send(partitionUpdateEvent.OutboxBatch);
        }

        void Send(Batch batch)
        {
            batch.ReadyToSendTimestamp = this.Partition.CurrentTimeMs;

            // now that we know the sending event is persisted, we can send the messages
            foreach (var outmessage in batch.OutgoingMessages)
            {
                DurabilityListeners.Register(outmessage, batch);
                outmessage.OriginPartition = this.Partition.PartitionId;
                outmessage.OriginPosition = batch.Position;
                this.Partition.Send(outmessage);
            }
        }

        [DataContract]
        public class Batch : TransportAbstraction.IDurabilityListener
        {
            [DataMember]
            public List<PartitionMessageEvent> OutgoingMessages { get; set; } = new List<PartitionMessageEvent>();

            [IgnoreDataMember]
            public long Position { get; set; }

            [IgnoreDataMember]
            public Partition Partition { get; set; }

            [IgnoreDataMember]
            int numAcks = 0;

            [IgnoreDataMember]
            public double? ProcessedTimestamp { get; set; }

            [IgnoreDataMember]
            public double ReadyToSendTimestamp { get; set; }
 
            public void ConfirmDurable(Event evt)
            {
                var partitionMessageEvent = (PartitionMessageEvent)evt;

                var workItemTraceHelper = this.Partition.WorkItemTraceHelper;
                if (workItemTraceHelper.TraceTaskMessages)
                {
                    double? persistenceDelayMs = this.ProcessedTimestamp.HasValue ? (this.ReadyToSendTimestamp - this.ProcessedTimestamp.Value) : null;
                    double sendDelayMs = this.Partition.CurrentTimeMs - this.ReadyToSendTimestamp;

                    foreach (var entry in partitionMessageEvent.TracedTaskMessages)
                    {
                        workItemTraceHelper.TraceTaskMessageSent(this.Partition.PartitionId, entry.message, entry.workItemId, persistenceDelayMs, sendDelayMs);
                    }
                }

                if (++this.numAcks == this.OutgoingMessages.Count)
                {
                    this.Partition.SubmitInternalEvent(new SendConfirmed()
                    {
                        PartitionId = this.Partition.PartitionId,
                        Position = Position,
                    });
                }
            }
        }

        public void Process(SendConfirmed evt, EffectTracker _)
        {
            this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Store has sent all outbound messages by event {evt} id={evt.EventIdString}");

            // we no longer need to keep these events around
            this.Outbox.Remove(evt.Position);
        }

        public void Process(ActivityCompleted evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.OutgoingMessages.Add(new RemoteActivityResultReceived()
            {
                PartitionId = evt.OriginPartitionId,
                Result = evt.Response,
                ActivityId = evt.ActivityId,
                ActivitiesQueueSize = evt.ReportedLoad,
            });
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            var batch = new Batch();
            int subPosition = 0;

            IEnumerable<(uint,TaskMessage)> Messages()
            {
                foreach (var message in evt.RemoteMessages)
                {
                    var instanceId = message.OrchestrationInstance.InstanceId;
                    var destination = this.Partition.PartitionFunction(instanceId);
                    yield return (destination, message);
                }
            }

            void AddMessage(TaskMessagesReceived outmessage, TaskMessage message)
            {
                if (Entities.IsDelayedEntityMessage(message, out _))
                {
                    (outmessage.DelayedTaskMessages ??= new List<TaskMessage>()).Add(message);
                }
                else if (message.Event is ExecutionStartedEvent executionStartedEvent && executionStartedEvent.ScheduledStartTime.HasValue)
                {
                    (outmessage.DelayedTaskMessages ??= new List<TaskMessage>()).Add(message);
                }
                else
                {
                    (outmessage.TaskMessages ??= new List<TaskMessage>()).Add(message);
                }
                outmessage.SubPosition = ++subPosition;
            }

            if (evt.PackPartitionTaskMessages > 1)
            {
                // pack multiple TaskMessages for the same destination into a single TaskMessagesReceived event
                var sorted = new Dictionary<uint, TaskMessagesReceived>();
                foreach ((uint destination, TaskMessage message) in Messages())
                {
                    if (!sorted.TryGetValue(destination, out var outmessage))
                    {
                        sorted[destination] = outmessage = new TaskMessagesReceived()
                        {
                            PartitionId = destination,
                            WorkItemId = evt.WorkItemId,
                        };
                    }

                    AddMessage(outmessage, message);

                    // send the message if we have reached the pack limit
                    if (outmessage.NumberMessages >= evt.PackPartitionTaskMessages)
                    {
                        batch.OutgoingMessages.Add(outmessage);
                        sorted.Remove(destination);
                    }
                }
                batch.OutgoingMessages.AddRange(sorted.Values);
            }
            else
            {
                // send each TaskMessage as a separate TaskMessagesReceived event
                foreach ((uint destination, TaskMessage message) in Messages())
                {
                    var outmessage = new TaskMessagesReceived()
                    {
                        PartitionId = destination,
                        WorkItemId = evt.WorkItemId,
                    };
                    AddMessage(outmessage, message);
                    batch.OutgoingMessages.Add(outmessage);
                }
            }
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public void Process(OffloadDecision evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.OutgoingMessages.Add(new ActivityOffloadReceived()
            {
                PartitionId = evt.DestinationPartitionId,
                OffloadedActivities = evt.OffloadedActivities,
                Timestamp = evt.Timestamp,
            });
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }
    }
}
