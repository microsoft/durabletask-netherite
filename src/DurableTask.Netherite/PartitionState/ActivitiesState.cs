// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Netherite.Scaling;

    [DataContract]
    class ActivitiesState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, ActivityInfo> Pending { get; private set; }

        [DataMember]
        public Queue<ActivityInfo> LocalBacklog { get; private set; }

        [DataMember]
        public Queue<ActivityInfo> QueuedRemotes { get; private set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        [DataMember]
        public int EstimatedWorkItemQueueSize { get; private set; }

        [DataMember]
        public int WorkItemQueueLimit { get; set; }

        [DataMember]
        public DateTime? LastSolicitation { get; set; }

        [DataMember]
        public double? AverageActivityCompletionTime { get; set; }

        [DataMember]
        public DateTime[] TransferCommandsReceived { get; set; }

        [IgnoreDataMember]
        DateTime LastSentToLoadMonitor = DateTime.MinValue;

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Activities);

        public static string GetWorkItemId(uint partition, long activityId) => $"{partition:D2}A{activityId}";

        public override void OnFirstInitialization()
        {
            this.Pending = new Dictionary<long, ActivityInfo>();
            // Backlog queue for local tasks
            this.LocalBacklog = new Queue<ActivityInfo>();
            // Queue for remote tasks
            this.QueuedRemotes = new Queue<ActivityInfo>();
            this.TransferCommandsReceived = new DateTime[this.Partition.NumberPartitions()];
            uint numberPartitions = this.Partition.NumberPartitions();
            this.WorkItemQueueLimit = 10;
        }

        const double SMOOTHING_FACTOR = 0.1;

        public override void Process(RecoveryCompleted evt, EffectTracker effects)
        {
            foreach (var kvp in this.Pending)
            {
                kvp.Value.DequeueCount++;

                if (!effects.IsReplaying)
                {
                    this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, kvp.Key, kvp.Value.Message, kvp.Value.OriginWorkItemId, evt));
                }
            }

            if (!effects.IsReplaying)
            {
                if (this.LocalBacklog.Count > 0)
                {
                    this.ScheduleNextOffloadDecision(TimeSpan.Zero);
                }
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Activities = this.Pending.Count + this.LocalBacklog.Count + this.QueuedRemotes.Count;

            if (info.Activities > 0)
            {
                var maxLatencyInQueue = Enumerable.Concat(Enumerable.Concat(this.LocalBacklog, this.QueuedRemotes), this.Pending.Values)
                    .Select(a => (long)(DateTime.UtcNow - a.IssueTime).TotalMilliseconds)
                    .DefaultIfEmpty()
                    .Max();

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

        // called frequently, directly from the StoreWorker. Must not modify any [DataMember] fields.
        public void CollectLoadMonitorInformation(DateTime now)
        {
            // send load information if there is a backlog of if it was solicited recently
            if (this.LocalBacklog.Count > 0  
                || this.QueuedRemotes.Count > 0
                || (now - this.LastSentToLoadMonitor) > TimeSpan.FromSeconds(10)
                || (this.LastSolicitation.HasValue 
                    && (now - this.LastSolicitation.Value) < LoadMonitor.SOLICITATION_VALIDITY))
            {          
                this.Partition.Send(new LoadInformationReceived()
                {
                    RequestId = Guid.NewGuid(),
                    PartitionId = this.Partition.PartitionId,
                    Stationary = this.Pending.Count + this.QueuedRemotes.Count,
                    Mobile = this.LocalBacklog.Count,
                    AverageActCompletionTime = this.AverageActivityCompletionTime,
                    TransfersReceived = (DateTime[]) this.TransferCommandsReceived.Clone()
                });
                this.LastSentToLoadMonitor = now;
            }
        }

        public override string ToString()
        {
            return $"Activities Pending.Count={this.Pending.Count} next={this.SequenceNumber:D6}";
        }

        void ScheduleNextOffloadDecision(TimeSpan delay)
        {
            if (ActivityScheduling.RequiresPeriodicOffloadDecision(this.Partition.Settings.ActivityScheduler))
            {
                this.Partition.PendingTimers.Schedule(DateTime.UtcNow + delay, new OffloadDecision()
                {
                    PartitionId = this.Partition.PartitionId,
                    Timestamp = DateTime.UtcNow + delay,
                });
            }
        }

        public bool TryGetNextActivity(out ActivityInfo activityInfo)
        {
            // take the most recent from the backlog or the queued remotes
            if (this.LocalBacklog.Count > 0 &&
                !(this.QueuedRemotes.Count > 0 && this.QueuedRemotes.Peek().IssueTime < this.LocalBacklog.Peek().IssueTime))
            {
                activityInfo = this.LocalBacklog.Dequeue();
                return true;
            }
            else if (this.QueuedRemotes.Count > 0)
            {
                activityInfo = this.QueuedRemotes.Dequeue();
                return true;
            }
            else
            {
                activityInfo = null;
                return false;
            }
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            // the completed orchestration work item can launch activities
            foreach (var msg in evt.ActivityMessages)
            {
                var activityInfo = new ActivityInfo()
                {
                    ActivityId = this.SequenceNumber++,
                    IssueTime = evt.Timestamp,
                    Message = msg,
                    OriginWorkItemId = evt.WorkItemId,
                };

                if (this.Pending.Count == 0 || this.EstimatedWorkItemQueueSize < this.WorkItemQueueLimit)
                {
                    activityInfo.DequeueCount++;
                    this.Pending.Add(activityInfo.ActivityId, activityInfo);

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityInfo.ActivityId, msg, evt.WorkItemId, evt));
                    }

                    this.EstimatedWorkItemQueueSize++;
                }
                else
                {
                    this.LocalBacklog.Enqueue(activityInfo);

                    if (!effects.IsReplaying)
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, msg, evt.WorkItemId, $"LocalBacklog@{this.LocalBacklog.Count}");
                        if (this.LocalBacklog.Count == 1)
                        {
                            this.ScheduleNextOffloadDecision(TimeSpan.FromMilliseconds(100));
                        }
                    }
                }
            }
        }

        public override void Process(ActivityTransferReceived evt, EffectTracker effects)
        {
            // may bring in offloaded activities from other partitions
            foreach (var msg in evt.TransferredActivities)
            {
                var activityInfo = new ActivityInfo()
                {
                    ActivityId = this.SequenceNumber++,
                    IssueTime = evt.Timestamp,
                    Message = msg.Item1,
                    OriginWorkItemId = msg.Item2
                };

                if (this.Pending.Count == 0 || this.EstimatedWorkItemQueueSize <= this.WorkItemQueueLimit)
                {
                    activityInfo.DequeueCount++;
                    this.Pending.Add(activityInfo.ActivityId, activityInfo);

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityInfo.ActivityId, msg.Item1, msg.Item2, evt));
                    }

                    this.EstimatedWorkItemQueueSize++;
                }
                else
                {
                    this.QueuedRemotes.Enqueue(activityInfo);

                    if (!effects.IsReplaying)
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, msg.Item1, msg.Item2, $"QueuedRemotes@{this.QueuedRemotes.Count}");
                    }
                }
            }

            if (this.TransferCommandsReceived[evt.OriginPartition] < evt.Timestamp)
            {
                this.TransferCommandsReceived[evt.OriginPartition] = evt.Timestamp;
            }
        }

        public override void Process(ActivityCompleted evt, EffectTracker effects)
        {
            // records the result of a finished activity

            this.Pending.Remove(evt.ActivityId);

            this.EstimatedWorkItemQueueSize = evt.ReportedLoad;

            // update the average activity completion time
            this.AverageActivityCompletionTime = !this.AverageActivityCompletionTime.HasValue ? evt.LatencyMs :
                SMOOTHING_FACTOR * evt.LatencyMs + (1 - SMOOTHING_FACTOR) * this.AverageActivityCompletionTime.Value;

            if (evt.OriginPartitionId == effects.PartitionId)
            {
                // the response can be delivered to a session on this partition
                effects.Add(TrackedObjectKey.Sessions);
            }
            else
            {
                // the response must be sent to a remote partition
                effects.Add(TrackedObjectKey.Outbox);
            }

            // now that an activity has completed, we can perhaps add more from the backlog
            while (this.Pending.Count == 0 || this.EstimatedWorkItemQueueSize < this.WorkItemQueueLimit)
            {
                if (this.TryGetNextActivity(out var activityInfo))
                {
                    activityInfo.DequeueCount++;
                    this.Pending.Add(activityInfo.ActivityId, activityInfo);

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityInfo.ActivityId, activityInfo.Message, activityInfo.OriginWorkItemId, evt));
                    }

                    this.EstimatedWorkItemQueueSize++;
                }
                else
                {
                    break;
                }
            }
        }

        public override void Process(TransferCommandReceived evt, EffectTracker effects)
        {
            evt.TransferredActivities = new List<(TaskMessage, string)>();

            for (int i = 0; i < evt.NumActivitiesToSend; i++)
            {
                // we might want to offload tasks in remote queue as well
                if (this.LocalBacklog.Count == 0)
                {
                    break;
                }

                var info = this.LocalBacklog.Dequeue();
                evt.TransferredActivities.Add((info.Message, info.OriginWorkItemId));
            }

            if (this.TransferCommandsReceived[evt.PartitionId] < evt.Timestamp)
            {
                this.TransferCommandsReceived[evt.PartitionId] = evt.Timestamp;
            }

            effects.EventTraceHelper?.TraceEventProcessingDetail($"Processed OffloadCommand, " +
                $"OffloadDestination={evt.TransferDestination}, NumActivitiesSent={evt.TransferredActivities.Count}");
            effects.Add(TrackedObjectKey.Outbox);
        }

        public override void Process(SolicitationReceived evt, EffectTracker effects)
        {
            this.LastSolicitation = evt.Timestamp;
        }

        public override void Process(OffloadDecision offloadDecisionEvent, EffectTracker effects)
        {
            // check for offload conditions and if satisfied, send batch to remote
            if (this.LocalBacklog.Count == 0)
            {
                return;
            }

            uint numberPartitions = this.Partition.NumberPartitions();
            int numberOffloadPerPart = (int)(this.LocalBacklog.Count / (this.Partition.NumberPartitions() - 1));

            // we are adding (nonpersisted) information to the event just as a way of passing it to the OutboxState
            offloadDecisionEvent.ActivitiesToTransfer = new SortedDictionary<uint, List<(TaskMessage, string)>>();

            Queue<ActivityInfo> keep = new Queue<ActivityInfo>();

            while (this.LocalBacklog.Count > 0)
            {
                ActivityInfo next = this.LocalBacklog.Dequeue();

                // we distribute activities round-robin across all partitions, based on their sequence number
                uint destination = (uint)(next.ActivityId % numberPartitions);

                if (destination == this.Partition.PartitionId)
                {
                    keep.Enqueue(next);
                }
                else
                {
                    if (!offloadDecisionEvent.ActivitiesToTransfer.TryGetValue(destination, out var list))
                    {
                        offloadDecisionEvent.ActivitiesToTransfer[destination] = list = new List<(TaskMessage, string)>();
                    }
                    list.Add((next.Message, next.OriginWorkItemId));
                }
            }

            this.LocalBacklog = keep; // these are the activities that should remain local

            if (offloadDecisionEvent.ActivitiesToTransfer.Count > 0)
            {
                // process this on OutboxState so the events get sent
                effects.Add(TrackedObjectKey.Outbox);
            }

            if (!effects.IsReplaying)
            {
                effects.EventTraceHelper?.TracePartitionOffloadDecision(this.EstimatedWorkItemQueueSize, this.Pending.Count, this.LocalBacklog.Count, -1, offloadDecisionEvent);

                if (this.LocalBacklog.Count > 0)
                {
                    this.ScheduleNextOffloadDecision(TimeSpan.FromSeconds(1));
                }
            }
        }

        [DataContract]
        public class ActivityInfo
        {
            [DataMember]
            public long ActivityId;

            [DataMember]
            public TaskMessage Message;

            [DataMember]
            public string OriginWorkItemId;

            [DataMember]
            public DateTime IssueTime;

            [DataMember]
            public int DequeueCount;
        }
    }
}
