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

        [DataMember]
        public TimeSpan? LoadMonitorInterval { get; set; }

        [IgnoreDataMember]
        DateTime? LastLoadInformationSent { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Activities);

        public static string GetWorkItemId(uint partition, long activityId) => $"{partition:D2}A{activityId}";

        public override void OnFirstInitialization()
        {
            this.Pending = new Dictionary<long, (TaskMessage, string)>();
            // Backlog queue for local tasks
            this.LocalBacklog = new Queue<ActivityInfo>();
            // Queue for remote tasks
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
        // xz: I feel like the max offload should be larger than the maxConcurrentActivities. e.g. 2*max
        const int OFFLOAD_MAX_BATCH_SIZE = 20;
        const int OFFLOAD_MIN_BATCH_SIZE = 10;

        // xz: not sure what this line means
        const int MAX_WORKITEM_LOAD = 10;

        public override void OnRecoveryCompleted()
        {
            // reschedule work items
            foreach (var pending in this.Pending)
            {
                this.Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, pending.Key, pending.Value.message, pending.Value.workItem));
            }

            if (this.LocalBacklog.Count > 0)
            {
                TimeSpan WaitTimeThresholdForOffload = this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.Aggressive ?
                        TimeSpan.FromMilliseconds(0) : TimeSpan.FromSeconds(10);
                this.ScheduleNextOffloadDecision(WaitTimeThresholdForOffload);
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Activities = this.Pending.Count + this.LocalBacklog.Count + this.QueuedRemotes.Count;

            if (info.Activities > 0)
            {
                var maxLatencyInQueue = Enumerable.Concat(this.LocalBacklog, this.QueuedRemotes)
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
        public void CollectLoadMonitorInformation()
        {
            if (!this.LastLoadInformationSent.HasValue  // always send info if we have not sent one since loading this partition
                || (this.LoadMonitorInterval.HasValue && ((DateTime.UtcNow - this.LastLoadInformationSent.Value) > this.LoadMonitorInterval.Value)))
            {
                this.Partition.Send(new LoadInformationReceived()
                {
                    RequestId = Guid.NewGuid(),
                    PartitionId = this.Partition.PartitionId,
                    BacklogSize = this.LocalBacklog.Count + this.QueuedRemotes.Count,
                });

                this.LastLoadInformationSent = DateTime.UtcNow;
            }
        }

        public override string ToString()
        {
            return $"Activities ({this.Pending.Count} pending) next={this.SequenceNumber:D6}";
        }

        void ScheduleNextOffloadDecision(TimeSpan delay)
        {
            if (this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.Local
                && this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.LoadMonitor)
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
                            TimeSpan WaitTimeThresholdForOffload = this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.Aggressive ?
                                TimeSpan.FromMilliseconds(0) : TimeSpan.FromSeconds(10);
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

            this.EstimatedLocalWorkItemLoad = evt.ReportedLoad;

            if (evt.OriginPartitionId == effects.Partition.PartitionId)
            {
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
            this.LoadMonitorInterval = TimeSpan.FromSeconds(1);
            this.CollectLoadMonitorInformation();
        }

        public void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            // records the reported queue size
            this.ReportedRemoteLoads[evt.OriginPartition] = evt.ActivitiesQueueSize;
        }

        public void Process(OffloadCommandReceived evt, EffectTracker effects)
        {
            // we can use this for tracing while we develop and debug the code
            this.Partition.EventTraceHelper.TraceEventProcessingWarning($"Processing OffloadCommand, " +
                $"OffloadDestination={evt.OffloadDestination}, NumActivitiesToSend={evt.NumActivitiesToSend}");

            evt.OffloadedActivities = new List<(TaskMessage, string)>();

            for (int i = 0; i < evt.NumActivitiesToSend; i++)
            {
                // we might want to offload tasks in remote queue as well
                if (this.LocalBacklog.Count == 0)
                {
                    break;
                }

                var info = this.LocalBacklog.Dequeue();
                evt.OffloadedActivities.Add((info.Message, info.WorkItemId));

            }


            this.Partition.EventTraceHelper.TraceEventProcessingWarning($"Processed OffloadCommand, " +
                $"OffloadDestination={evt.OffloadDestination}, NumActivitiesSent={evt.OffloadedActivities.Count}");
            effects.Add(TrackedObjectKey.Outbox);
        }

        public void Process(ProbingControlReceived evt, EffectTracker effects)
        {
            this.LoadMonitorInterval = evt.LoadMonitorInterval;
        }

        public void Process(OffloadDecision offloadDecisionEvent, EffectTracker effects)
        {
            // check for offload conditions and if satisfied, send batch to remote
            if (this.LocalBacklog.Count == 0)
            {
                return;
            }

            if (this.Partition.Settings.ActivityScheduler == ActivitySchedulerOptions.Aggressive)
            {
                uint numberPartitions = this.Partition.NumberPartitions();
                int numberOffloadPerPart = (int)(this.LocalBacklog.Count / (this.Partition.NumberPartitions() - 1));
                // we are adding (nonpersisted) information to the event just as a way of passing it to the OutboxState
                offloadDecisionEvent.OffloadedActivities = new Dictionary<uint, List<(TaskMessage, string)>>();

                foreach (uint target in Enumerable.Range(0, (int)numberPartitions))
                {
                    if (target == this.Partition.PartitionId)
                    {
                        continue;
                    }

                    var ActivitiesToOffload = new List<(TaskMessage, string)>();
                    for (int i = 0; i < numberOffloadPerPart; i++)
                    {
                        var info = this.LocalBacklog.Dequeue();
                        ActivitiesToOffload.Add((info.Message, info.WorkItemId));

                        if (this.LocalBacklog.Count == 0)
                        {
                            break;
                        }
                    }

                    offloadDecisionEvent.OffloadedActivities.Add(target, ActivitiesToOffload);
                }


                // process this on OutboxState so the events get sent
                effects.Add(TrackedObjectKey.Outbox);

                // this may not be the most concise way to format the message. 
                var offloadCountPerPartition = new List<int>(new int[numberPartitions]);
                offloadCountPerPartition[(int)this.Partition.PartitionId] = -1;
                foreach (var kvp in offloadDecisionEvent.OffloadedActivities)
                {
                    offloadCountPerPartition[(int)kvp.Key] = kvp.Value.Count;
                }

                var reportedRemotes = string.Join(",", offloadCountPerPartition.Select((int x) => x.ToString()));

                this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.Pending.Count, this.LocalBacklog.Count, -1, reportedRemotes);
            }
            else
            {
                // find how many offload candidate tasks we have
                int numberOffloadCandidates = this.CountOffloadCandidates(offloadDecisionEvent.Timestamp);

                if (numberOffloadCandidates < OFFLOAD_MIN_BATCH_SIZE)
                {
                    return; // no offloading if we cannot offload enough
                }

                if (this.FindOffloadTarget(offloadDecisionEvent.Timestamp, numberOffloadCandidates, out uint target, out int maxBatchsize))
                {
                    // don't pick this same target again until we get a response telling us the current queue size
                    var targetLoad = this.ReportedRemoteLoads[target];
                    this.ReportedRemoteLoads[target] = RESPONSE_PENDING;

                    // we are adding (nonpersisted) information to the event just as a way of passing it to the OutboxState
                    offloadDecisionEvent.OffloadedActivities = new Dictionary<uint, List<(TaskMessage, string)>>();
                    var ActivitiesToOffload = new List<(TaskMessage, string)>();

                    for (int i = 0; i < maxBatchsize; i++)
                    {
                        var info = this.LocalBacklog.Dequeue();
                        ActivitiesToOffload.Add((info.Message, info.WorkItemId));

                        TimeSpan WaitTimeThresholdForOffload = this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.Aggressive ?
                                    TimeSpan.FromMilliseconds(0) : TimeSpan.FromSeconds(10);
                        if (this.LocalBacklog.Count == 0 || offloadDecisionEvent.Timestamp - this.LocalBacklog.Peek().IssueTime < WaitTimeThresholdForOffload)
                        {
                            break;
                        }
                    }

                    offloadDecisionEvent.OffloadedActivities.Add(target, ActivitiesToOffload);

                    // process this on OutboxState so the events get sent
                    effects.Add(TrackedObjectKey.Outbox);

                    var reportedRemotes = string.Join(",", this.ReportedRemoteLoads.Select((int x, int i) =>
                        (i == target) ? $"{targetLoad}+{offloadDecisionEvent.OffloadedActivities.Count}" : (x == NOT_CONTACTED ? "-" : (x == RESPONSE_PENDING ? "X" : x.ToString()))));

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.Pending.Count, this.LocalBacklog.Count, this.QueuedRemotes.Count, reportedRemotes);

                        // try again relatively soon
                        this.ScheduleNextOffloadDecision(TimeSpan.FromMilliseconds(50));
                    }
                }
                else
                {
                    var reportedRemotes = string.Join(",", this.ReportedRemoteLoads.Select((int x) => x == NOT_CONTACTED ? "-" : (x == RESPONSE_PENDING ? "X" : x.ToString())));

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.Pending.Count, this.LocalBacklog.Count, this.QueuedRemotes.Count, reportedRemotes);

                        // there are no eligible recipients... try again in a while
                        this.ScheduleNextOffloadDecision(TimeSpan.FromSeconds(10));
                    }
                }
            }
        }

        int CountOffloadCandidates(DateTime now)
        {
            int numberOffloadCandidates = 0;
            int limit = (int)(Math.Min(PORTION_SUBJECT_TO_OFFLOAD * this.LocalBacklog.Count, OFFLOAD_MAX_BATCH_SIZE * this.Partition.NumberPartitions()));
            foreach (var entry in this.LocalBacklog)
            {
                TimeSpan WaitTimeThresholdForOffload = this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.Aggressive ?
                        TimeSpan.FromMilliseconds(0) : TimeSpan.FromSeconds(10);
                if (now - entry.IssueTime < WaitTimeThresholdForOffload
                    || numberOffloadCandidates++ > limit)
                {
                    break;
                }
            }
            return numberOffloadCandidates;
        }

        bool FindOffloadTarget(DateTime timeStamp, double portionSubjectToOffload, out uint target, out int batchsize)
        {
            Random rand = new Random((int)timeStamp.Ticks);
            switch (this.Partition.Settings.ActivityScheduler)
            {
                case ActivitySchedulerOptions.PeriodicOffloadThreshold:
                    return this.FindOffloadTargetThreshold(portionSubjectToOffload, out target, out batchsize);

                case ActivitySchedulerOptions.PeriodicOffloadRandom:
                    return this.FindOffloadTargetRandom(rand, portionSubjectToOffload, out target, out batchsize);

                default:
                    return this.FindOffloadTargetRandom(rand, portionSubjectToOffload, out target, out batchsize);
            }

        }

        bool FindOffloadTargetRandom(Random rand, double portionSubjectToOffload, out uint target, out int batchsize)
        {
            uint numberPartitions = this.Partition.NumberPartitions();
            target = this.Partition.PartitionId;

            // pick a random partition to offload tasks
            while (target == this.Partition.PartitionId)
            {
                target = (uint)rand.Next(0, (int)numberPartitions);
            }

            batchsize = Math.Min(OFFLOAD_MAX_BATCH_SIZE, Math.Max(OFFLOAD_MIN_BATCH_SIZE, (int)Math.Ceiling(portionSubjectToOffload)));
            return true;
        }


        bool FindOffloadTargetThreshold(double portionSubjectToOffload, out uint target, out int batchsize)
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
