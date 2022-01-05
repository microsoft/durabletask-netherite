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
    using DurableTask.Netherite.Scaling;

    [DataContract]
    class OutboxState : TrackedObject, TransportAbstraction.IDurabilityListener
    {
        [DataMember]
        public SortedDictionary<long, Batch> Outbox { get; private set; } = new SortedDictionary<long, Batch>();

        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        public override void OnRecoveryCompleted(EffectTracker effects, RecoveryCompleted evt)
        {
            // resend all pending
            foreach (var kvp in this.Outbox)
            {
                // recover non-persisted fields
                kvp.Value.Position = kvp.Key;
                kvp.Value.Partition = this.Partition;

                // resend (anything we have recovered is of course persisted)
                effects.EventDetailTracer?.TraceEventProcessingDetail($"Resent batch {kvp.Key:D10} ({kvp.Value.OutgoingMessages.Count} messages, {kvp.Value.OutgoingResponses.Count} responses)");
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

            foreach (var partitionMessageEvent in batch.OutgoingMessages)
            {
                partitionMessageEvent.OriginPartition = this.Partition.PartitionId;
                partitionMessageEvent.OriginPosition = commitPosition;
            }

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
                this.Partition.Send(outmessage);
            }
            foreach (var outresponse in batch.OutgoingResponses)
            {
                DurabilityListeners.Register(outresponse, batch);
                this.Partition.Send(outresponse);
            }
        }

        [DataContract]
        public class Batch : TransportAbstraction.IDurabilityListener
        {
            [DataMember]
            public List<PartitionMessageEvent> OutgoingMessages { get; set; } = new List<PartitionMessageEvent>();

            [DataMember]
            public List<ClientEvent> OutgoingResponses { get; set; } = new List<ClientEvent>();

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
                if (evt is PartitionMessageEvent partitionMessageEvent)
                {
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
                }

                if (++this.numAcks == this.OutgoingMessages.Count + this.OutgoingResponses.Count)
                {
                    this.Partition.SubmitEvent(new SendConfirmed()
                    {
                        PartitionId = this.Partition.PartitionId,
                        Position = Position,
                    });
                }
            }
        }

        public override void Process(SendConfirmed evt, EffectTracker effects)
        {
            effects.EventDetailTracer?.TraceEventProcessingDetail($"Store has sent all outbound messages by event {evt} id={evt.EventIdString}");

            // we no longer need to keep these events around
            this.Outbox.Remove(evt.Position);
        }

        public override void Process(ActivityCompleted evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.OutgoingMessages.Add(new RemoteActivityResultReceived()
            {
                PartitionId = evt.OriginPartitionId,
                Result = evt.Response,
                Timestamp = evt.Timestamp,
                LatencyMs = evt.LatencyMs,
                ActivityId = evt.ActivityId,
            });
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            var batch = new Batch();
            int subPosition = 0;

            bool sendResponses = evt.ResponsesToSend != null;
            bool sendMessages = evt.RemoteMessages?.Count > 0;

            if (! (sendResponses || sendMessages))
            {
                return;
            }

            if (sendResponses)
            {
                foreach(var r in evt.ResponsesToSend)
                {
                    batch.OutgoingResponses.Add(r);
                }
            }

            if (sendMessages)
            {
                IEnumerable<(uint, TaskMessage)> Messages()
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
            }

            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public override void Process(OffloadDecision evt, EffectTracker effects)
        {
            var batch = new Batch();

            foreach(var kvp in evt.ActivitiesToTransfer)
            {
                batch.OutgoingMessages.Add(new ActivityTransferReceived()
                {
                    PartitionId=kvp.Key,
                    TransferredActivities = kvp.Value,
                    Timestamp = evt.Timestamp,
                });
            }

            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public override void Process(TransferCommandReceived evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.OutgoingMessages.Add(new ActivityTransferReceived()
            {
                PartitionId = evt.TransferDestination,
                TransferredActivities = evt.TransferredActivities,
                Timestamp = evt.Timestamp,
            });

            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public override void Process(WaitRequestReceived evt, EffectTracker effects)
        {
            this.Partition.Assert(evt.ResponseToSend != null);
            var batch = new Batch();
            batch.OutgoingResponses.Add(evt.ResponseToSend);
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public override void Process(CreationRequestReceived evt, EffectTracker effects)
        {
            this.Partition.Assert(evt.ResponseToSend != null);
            var batch = new Batch();
            batch.OutgoingResponses.Add(evt.ResponseToSend);
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public override void Process(DeletionRequestReceived evt, EffectTracker effects)
        {
            this.Partition.Assert(evt.ResponseToSend != null);
            var batch = new Batch();
            batch.OutgoingResponses.Add(evt.ResponseToSend);
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }
    }
}
