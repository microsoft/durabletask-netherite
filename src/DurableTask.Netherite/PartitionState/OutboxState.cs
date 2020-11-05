// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
    using DurableTask.Netherite.Scaling;

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

            // Update the ready to send timestamp to check the delay caused 
            // by non-speculation
            evt.ReadyToSendTimestamp = this.Partition.CurrentTimeMs;

            this.Outbox[commitPosition] = batch;
            batch.Position = commitPosition;
            batch.Partition = this.Partition;

            if (!effects.IsReplaying)
            {
                if (!this.Partition.Settings.PersistStepsFirst)
                {
                    // we must not send messages until this step has been persisted
                    evt.OutboxBatch = batch;
                    DurabilityListeners.Register(evt, this);
                }
                else
                {
                    // we can send the messages now
                    this.Send(batch);
                }
            }
        }

        public void ConfirmDurable(Event evt)
        {
            var partitionUpdateEvent = ((PartitionUpdateEvent)evt);

            // Calculate the delay by not sending immediately
            partitionUpdateEvent.SentTimestamp = this.Partition.CurrentTimeMs;
            this.Send(partitionUpdateEvent.OutboxBatch);
        }

        void Send(Batch batch)
        {
            // now that we know the sending event is persisted, we can send the messages
            foreach (var outmessage in batch.OutgoingMessages)
            {
                DurabilityListeners.Register(outmessage, batch);
                outmessage.OriginPartition = this.Partition.PartitionId;
                outmessage.OriginPosition = batch.Position;
                //outmessage.SentTimestampUnixMs = DateTimeOffset.Now.ToUnixTimeMilliseconds();
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

            public void ConfirmDurable(Event evt)
            {
                this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Transport has confirmed event {evt} id={evt.EventIdString}");

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
            var sorted = new Dictionary<uint, TaskMessagesReceived>();
            foreach (var message in evt.RemoteMessages)
            {   
                var instanceId = message.OrchestrationInstance.InstanceId;
                var destination = this.Partition.PartitionFunction(instanceId);          
                if (!sorted.TryGetValue(destination, out var outmessage))
                {
                    sorted[destination] = outmessage = new TaskMessagesReceived()
                    {
                        PartitionId = destination,
                        WorkItemId = evt.WorkItemId,
                    };
                }
                if (Entities.IsDelayedEntityMessage(message, out _))
                {
                    (outmessage.DelayedTaskMessages ?? (outmessage.DelayedTaskMessages = new List<TaskMessage>())).Add(message);
                }
                else if (message.Event is ExecutionStartedEvent executionStartedEvent && executionStartedEvent.ScheduledStartTime.HasValue)
                {
                    (outmessage.DelayedTaskMessages ?? (outmessage.DelayedTaskMessages = new List<TaskMessage>())).Add(message);
                }
                else
                {
                    (outmessage.TaskMessages ?? (outmessage.TaskMessages = new List<TaskMessage>())).Add(message);
                }
            }
            var batch = new Batch();
            batch.OutgoingMessages.AddRange(sorted.Values);
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
