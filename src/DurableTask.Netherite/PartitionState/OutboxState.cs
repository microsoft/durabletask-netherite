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
                this.SendAllMessages(kvp.Value);
                kvp.Value.SendPersistenceConfirmation();
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

        void SendBatch(PartitionUpdateEvent evt, EffectTracker effects, Batch batch)
        {
            // put the messages in the outbox where they are kept until all messages are sent
            var commitPosition = evt.NextCommitLogPosition;

            // Update the ready to send timestamp to check the delay caused 
            // by conservative persistence
            evt.ReadyToSendTimestamp = this.Partition.CurrentTimeMs;

            this.Outbox[commitPosition] = batch;
            batch.Position = commitPosition;
            batch.Partition = this.Partition;

            if (!effects.IsReplaying)
            {
                if (evt is BatchProcessed batchProcessedEvt)
                {
                    if (batchProcessedEvt.PersistenceStatus == BatchProcessed.BatchPersistenceStatus.GloballyPipelined)
                    {
                        // in this case the event is sent before it is persisted, and we send a confirmation later
                        batch.EventRequiringPersistence = evt;
                        this.SendAllMessages(batch);
                        return;
                    }
                    else if (batchProcessedEvt.PersistenceStatus == BatchProcessed.BatchPersistenceStatus.Persisted)
                    {
                        // in this case the event is already persisted so we can send right away
                        this.SendAllMessages(batch);
                        return;
                    }
                    else
                    {
                        this.Partition.Assert(batchProcessedEvt.PersistenceStatus == BatchProcessed.BatchPersistenceStatus.LocallyPipelined);
                    }
                }
                
                // we cannot send until persistence is confirmed.
                // So we register for a durability notification, at which point we will send the batch.
                evt.OutboxBatch = batch;
                DurabilityListeners.Register(evt, this);
            }
        }

        public void ConfirmDurable(Event evt)
        {
            var partitionUpdateEvent = ((PartitionUpdateEvent)evt);

            // Calculate the delay by not sending immediately
            partitionUpdateEvent.SentTimestamp = this.Partition.CurrentTimeMs;
            this.SendAllMessages(partitionUpdateEvent.OutboxBatch);
        }

        void SendAllMessages(Batch batch)
        {
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
            public BatchProcessed.BatchPersistenceStatus PersistenceStatus { get; set; }

            [IgnoreDataMember]
            public long Position { get; set; }

            [IgnoreDataMember]
            public Partition Partition { get; set; }

            [IgnoreDataMember]
            int numAcks = 0;

            [IgnoreDataMember]
            public Event EventRequiringPersistence;

            public void ConfirmDurable(Event evt)
            {
                if (evt == this.EventRequiringPersistence)
                {
                    this.Partition.Assert(this.PersistenceStatus == BatchProcessed.BatchPersistenceStatus.GloballyPipelined);
                    this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"LogWorker has confirmed durability of event {evt} id={evt.EventIdString}");
                    this.EventRequiringPersistence = null;
                    this.SendPersistenceConfirmation();
                }
                else
                {
                    this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Transport has confirmed event {evt} id={evt.EventIdString}");
                    ++this.numAcks;
                }

                if (this.numAcks == this.OutgoingMessages.Count && this.EventRequiringPersistence == null)
                {
                    // remove this batch safely from the outbox via an event
                    this.Partition.SubmitInternalEvent(new SendConfirmed()
                    {
                        PartitionId = this.Partition.PartitionId,
                        Position = Position,
                    });
                }
            }

            public void SendPersistenceConfirmation()
            {
                if (this.PersistenceStatus == BatchProcessed.BatchPersistenceStatus.GloballyPipelined)
                {
                    var destinationPartitionIds = this.OutgoingMessages.Select(m => m.PartitionId).Distinct();
                    foreach (var destinationPartitionId in destinationPartitionIds)
                    {
                        var persistenceConfirmationEvent = new PersistenceConfirmationEvent
                        {
                            PartitionId = destinationPartitionId,
                            OriginPartition = this.Partition.PartitionId,
                            OriginPosition = this.Position,
                        };

                        this.Partition.Send(persistenceConfirmationEvent);
                    }
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
            this.SendBatch(evt, effects, batch);
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.PersistenceStatus = evt.PersistenceStatus;
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
                            PersistenceStatus = batch.PersistenceStatus,
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
                        PersistenceStatus = batch.PersistenceStatus,
                    };
                    AddMessage(outmessage, message);
                    batch.OutgoingMessages.Add(outmessage);
                }
            }
            this.SendBatch(evt, effects, batch);
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
            this.SendBatch(evt, effects, batch);
        }
    }
}
