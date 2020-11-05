// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Scaling;
    using Dynamitey;

    [DataContract]
    class ActivitiesState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, (TaskMessage message, string workItem)> Pending { get; private set; }

        [DataMember]
        public Queue<ActivityInfo> LocalBacklog { get; private set; } 

        [DataMember]
        public Queue<ActivityInfo> QueuedRemotes { get; private set; }

        [DataMember]
        public int[] ReportedRemoteLoads { get; private set; }

        [DataMember]
        public int EstimatedLocalWorkItemLoad { get; private set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Activities);

        public static string GetWorkItemId(uint partition, long activityId) => $"{partition:D2}A{activityId}";

        public override void OnFirstInitialization()
        {
            this.Pending = new Dictionary<long, (TaskMessage, string)>();
            this.LocalBacklog = new Queue<ActivityInfo>();
            this.QueuedRemotes = new Queue<ActivityInfo>();
            this.ReportedRemoteLoads = new int[this.Partition.NumberPartitions()];
            uint numberPartitions = this.Partition.NumberPartitions();
            for (uint i = 0; i < numberPartitions; i++)
            {
                this.ReportedRemoteLoads[i] = NOT_CONTACTED;
            }
        }

        const int NOT_CONTACTED = -1;
        const int RESPONSE_PENDING = int.MaxValue;

        const int ABSOLUTE_LOAD_LIMIT_FOR_REMOTES = 1000;
        const double RELATIVE_LOAD_LIMIT_FOR_REMOTES = .8;
        const double PORTION_SUBJECT_TO_OFFLOAD = .5;
        const int OFFLOAD_MAX_BATCH_SIZE = 100;
        const int OFFLOAD_MIN_BATCH_SIZE = 10;

        const int MAX_WORKITEM_LOAD = 10;

        // minimum time for an activity to be waiting before it is offloaded to a remote partition
        static readonly TimeSpan WaitTimeThresholdForOffload = TimeSpan.FromSeconds(15);

        public override void OnRecoveryCompleted()
        {
            // reschedule work items
            foreach (var pending in this.Pending)
            {
                this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, pending.Key, pending.Value.message, pending.Value.workItem));
            }

            if (this.LocalBacklog.Count > 0)
            {
                this.ScheduleNextOffloadDecision(WaitTimeThresholdForOffload);
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Activities = this.Pending.Count + this.LocalBacklog.Count + this.QueuedRemotes.Count;
            info.ActivityLatencyMs = Enumerable.Concat(this.LocalBacklog, this.QueuedRemotes)
                .Select(a => (long)(DateTime.UtcNow - a.IssueTime).TotalMilliseconds)
                .DefaultIfEmpty()
                .Max();
        }

        public override string ToString()
        {
            return $"Activities ({this.Pending.Count} pending) next={this.SequenceNumber:D6}";
        }

        void ScheduleNextOffloadDecision(TimeSpan delay)
        {
            this.Partition.PendingTimers.Schedule(DateTime.UtcNow + delay, new OffloadDecision()
            {
                PartitionId = this.Partition.PartitionId,
                Timestamp = DateTime.UtcNow + delay,
            });
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

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // the completed orchestration work item can launch activities
            foreach (var msg in evt.ActivityMessages)
            {
                var activityId = this.SequenceNumber++;

                if (this.Pending.Count == 0 || this.EstimatedLocalWorkItemLoad <= MAX_WORKITEM_LOAD)
                {
                    this.Pending.Add(activityId, (msg, evt.WorkItemId));

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityId, msg, evt.WorkItemId));
                    }

                    this.EstimatedLocalWorkItemLoad++;
                }
                else
                {
                    this.LocalBacklog.Enqueue(new ActivityInfo()
                    {
                        ActivityId = activityId,
                        IssueTime = evt.Timestamp,
                        Message = msg,
                        WorkItemId = evt.WorkItemId,
                    });

                    if (!effects.IsReplaying)
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, msg, evt.WorkItemId, $"LocalBacklog@{this.LocalBacklog.Count}");

                        if (this.LocalBacklog.Count == 1)
                        {
                            this.ScheduleNextOffloadDecision(WaitTimeThresholdForOffload);
                        }
                    }
                }
            }
        }

        public void Process(ActivityOffloadReceived evt, EffectTracker effects)
        {
            // may bring in offloaded activities from other partitions
            foreach (var msg in evt.OffloadedActivities)
            {
                var activityId = this.SequenceNumber++;

                if (this.Pending.Count == 0 || this.EstimatedLocalWorkItemLoad <= MAX_WORKITEM_LOAD)
                {
                    this.Pending.Add(activityId, msg);

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityId, msg.Item1, msg.Item2));
                    }

                    this.EstimatedLocalWorkItemLoad++;
                }
                else
                {
                    this.QueuedRemotes.Enqueue(new ActivityInfo()
                    {
                        ActivityId = activityId,
                        IssueTime = evt.Timestamp,
                        Message = msg.Item1,
                        WorkItemId = msg.Item2
                    });

                    if (!effects.IsReplaying)
                    {
                        this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, msg.Item1, msg.Item2, $"QueuedRemotes@{this.QueuedRemotes.Count}");
                    }
                }
            }
        }

        public void Process(ActivityCompleted evt, EffectTracker effects)
        {
            // records the result of a finished activity and launches an offload decision

            this.Pending.Remove(evt.ActivityId);

            if (evt.OriginPartitionId == effects.Partition.PartitionId)
            {
                this.EstimatedLocalWorkItemLoad = evt.ReportedLoad;
                // the response can be delivered to a session on this partition
                effects.Add(TrackedObjectKey.Sessions);
            }
            else
            {
                // the response must be sent to a remote partition
                evt.ReportedLoad = this.LocalBacklog.Count + this.QueuedRemotes.Count;
                effects.Add(TrackedObjectKey.Outbox);
            }

            // now that an activity has completed, we can perhaps add more from the backlog
            while (this.Pending.Count == 0 || this.EstimatedLocalWorkItemLoad <= MAX_WORKITEM_LOAD)
            {
                if (this.TryGetNextActivity(out var activityInfo))
                {
                    this.Pending.Add(activityInfo.ActivityId, (activityInfo.Message, activityInfo.WorkItemId));

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityInfo.ActivityId, activityInfo.Message, activityInfo.WorkItemId));
                    }

                    this.EstimatedLocalWorkItemLoad++;
                }
                else
                {
                    break;
                }
            }
        }

        public void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            // records the reported queue size
            this.ReportedRemoteLoads[evt.OriginPartition] = evt.ActivitiesQueueSize;
        }

        public void Process(OffloadDecision offloadDecisionEvent, EffectTracker effects)
        {
            // check for offload conditions and if satisfied, send batch to remote

            if (this.LocalBacklog.Count == 0)
            {
                return;
            }

            // find how many offload candidates we have
            int numberOffloadCandidates = this.CountOffloadCandidates(offloadDecisionEvent.Timestamp);

            if (numberOffloadCandidates < OFFLOAD_MIN_BATCH_SIZE)
            {
                return; // no offloading if we cannot offload enough
            }

            if (this.FindOffloadTarget(numberOffloadCandidates, out uint target, out int maxBatchsize))
            {
                // don't pick this same target again until we get a response telling us the current queue size
                var targetLoad = this.ReportedRemoteLoads[target];
                this.ReportedRemoteLoads[target] = RESPONSE_PENDING;

                // we are adding (nonpersisted) information to the event just as a way of passing it to the OutboxState
                offloadDecisionEvent.DestinationPartitionId = target;
                offloadDecisionEvent.OffloadedActivities = new List<(TaskMessage,string)>();

                for (int i = 0; i < maxBatchsize; i++)
                {
                    var info = this.LocalBacklog.Dequeue();
                    offloadDecisionEvent.OffloadedActivities.Add((info.Message, info.WorkItemId));

                    if (this.LocalBacklog.Count == 0 || offloadDecisionEvent.Timestamp - this.LocalBacklog.Peek().IssueTime < WaitTimeThresholdForOffload)
                    {
                        break;
                    }
                }

                // process this on OutboxState so the events get sent
                effects.Add(TrackedObjectKey.Outbox);

                var reportedRemotes = string.Join(",", this.ReportedRemoteLoads.Select((int x, int i) =>
                    (i == target) ? $"{targetLoad}+{offloadDecisionEvent.OffloadedActivities.Count}" : (x == NOT_CONTACTED ? "-" : (x == RESPONSE_PENDING ? "X" : x.ToString()))));

                this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.Pending.Count, this.LocalBacklog.Count, this.QueuedRemotes.Count, reportedRemotes);

                // try again relatively soon
                this.ScheduleNextOffloadDecision(TimeSpan.FromMilliseconds(200));
            }
            else
            {
                var reportedRemotes = string.Join(",", this.ReportedRemoteLoads.Select((int x) => x == NOT_CONTACTED ? "-" : (x == RESPONSE_PENDING ? "X" : x.ToString())));

                this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.Pending.Count, this.LocalBacklog.Count, this.QueuedRemotes.Count, reportedRemotes);

                // there are no eligible recipients... try again in a while
                this.ScheduleNextOffloadDecision(TimeSpan.FromSeconds(10));
            }
        }

        int CountOffloadCandidates(DateTime now)
        {
            int numberOffloadCandidates = 0;
            int limit = (int)(Math.Min(PORTION_SUBJECT_TO_OFFLOAD * this.LocalBacklog.Count, OFFLOAD_MAX_BATCH_SIZE * this.Partition.NumberPartitions()));
            foreach (var entry in this.LocalBacklog)
            {
                if (now - entry.IssueTime < WaitTimeThresholdForOffload
                    || numberOffloadCandidates++ > limit)
                {
                    break;
                }
            }
            return numberOffloadCandidates;
        }

        bool FindOffloadTarget(double portionSubjectToOffload, out uint target, out int batchsize)
        {
            uint numberPartitions = this.Partition.NumberPartitions();
            uint? firstNotContacted = null;
            uint? eligibleTargetWithSmallestQueue = null;
            int minimalReportedQueueSizeFound = int.MaxValue;
            double estimatedParallism = 0;
            double remoteLoadLimit = Math.Min(ABSOLUTE_LOAD_LIMIT_FOR_REMOTES, RELATIVE_LOAD_LIMIT_FOR_REMOTES * this.LocalBacklog.Count);

            for (uint i = 0; i < numberPartitions - 1; i++)
            {
                uint candidate = (this.Partition.PartitionId + i + 1) % numberPartitions;
                int reported = this.ReportedRemoteLoads[candidate];
                if (reported == NOT_CONTACTED)
                {
                    if (!firstNotContacted.HasValue)
                    {
                        firstNotContacted = candidate;
                    }
                    estimatedParallism += 1;
                }
                else if (reported != RESPONSE_PENDING)
                {
                    if (reported < remoteLoadLimit)
                    {
                        estimatedParallism += 1;
                        if (reported < minimalReportedQueueSizeFound)
                        {
                            minimalReportedQueueSizeFound = reported;
                            eligibleTargetWithSmallestQueue = candidate;
                        }
                    }
                }
                else
                {
                    estimatedParallism += 1;
                }
            }

            if (eligibleTargetWithSmallestQueue.HasValue)
            {
                // we found a lowly loaded target
                target = eligibleTargetWithSmallestQueue.Value;
                batchsize = Math.Min(OFFLOAD_MAX_BATCH_SIZE, Math.Max(OFFLOAD_MIN_BATCH_SIZE, (int)Math.Ceiling(portionSubjectToOffload / estimatedParallism)));
                if (minimalReportedQueueSizeFound < batchsize)
                {
                    return true;
                }
            }

            if (firstNotContacted.HasValue)
            {
                // we did not find a lowly loaded target with enough spare capacity
                target = firstNotContacted.Value;
                batchsize = OFFLOAD_MIN_BATCH_SIZE;
                return true;
            }

            target = 0;
            batchsize = 0;
            return false;
        }

        [DataContract]
        public class ActivityInfo
        {
            [DataMember]
            public long ActivityId;

            [DataMember]
            public TaskMessage Message;

            [DataMember]
            public string WorkItemId;

            [DataMember]
            public DateTime IssueTime;
        }
    }
}
