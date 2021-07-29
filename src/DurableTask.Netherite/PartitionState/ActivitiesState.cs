// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
    class ActivitiesState : TrackedObject, TransportAbstraction.IDurabilityListener
    {
        [DataMember]
        public Dictionary<long, (TaskMessage message, string workItem)> LocalPending { get; private set; }

        [DataMember]
        public Dictionary<(string originWorkItem, long sequenceNumber), ActivityInfo> RemotePending { get; private set; }
        
        [DataMember]
        public Queue<ActivityInfo> Backlog { get; private set; } 

        [DataMember]
        public int EstimatedLocalWorkItemLoad { get; private set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Activities);

        public static string GetWorkItemId(uint partition, long activityId) => $"{partition:D2}A{activityId}";

        public override void OnFirstInitialization()
        {
            this.LocalPending = new Dictionary<long, (TaskMessage, string)>();
            this.RemotePending = new Dictionary<(string originWorkItem, long sequenceNumber), ActivityInfo>();
            this.Backlog = new Queue<ActivityInfo>();
        }

        // Current design: Use bounded buffers for both pending local and pending remote activities.
        // In this preliminary design the buffer size is a constant. We will make smarter choices at some point.
        const int MAX_LOCAL_PENDING = 100;
        const int MAX_REMOTE_PENDING = 200;

        public override void OnRecoveryCompleted()
        {
            // reschedule work items
            foreach (var pending in this.LocalPending)
            {
                this.Partition.EnqueueActivityWorkItem(new ActivityLocalWorkItem(this.Partition, pending.Key, pending.Value.message, pending.Value.workItem));
            }

            foreach (var pending in this.RemotePending)
            {
                if (!pending.Value.Sent)
                {
                    this.Send(pending.Value);
                }
            }
            //if (this.Backlog.Count > 0)
            //{
            //    this.ScheduleNextOffloadDecision(WaitTimeThresholdForOffload);
            //}
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Activities = this.LocalPending.Count + this.RemotePending.Count + this.Backlog.Count;

            if (info.Activities > 0)
            {
                var maxLatencyInQueue = Enumerable.Concat(this.Backlog, this.RemotePending.Values)
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

        public override string ToString()
        {
            return $"Activities ({this.LocalPending.Count} pending) next={this.SequenceNumber:D6}";
        }

        //void ScheduleNextOffloadDecision(TimeSpan delay)
        //{
        //    if (this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.Local)
        //    {
        //        this.Partition.PendingTimers.Schedule(DateTime.UtcNow + delay, new OffloadDecision()
        //        {
        //            PartitionId = this.Partition.PartitionId,
        //            Timestamp = DateTime.UtcNow + delay,
        //        });
        //    }
        //}

        public bool TryGetNextActivity(out ActivityInfo activityInfo)
        {
            // take the most recent from the backlog or the queued remotes
            if (this.Backlog.Count > 0)
            {
                activityInfo = this.Backlog.Dequeue();
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

                if (this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.RemoteOnly
                    && (this.LocalPending.Count == 0 || this.EstimatedLocalWorkItemLoad <= MAX_LOCAL_PENDING))
                {
                    this.LocalPending.Add(activityId, (msg, evt.WorkItemId));

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityLocalWorkItem(this.Partition, activityId, msg, evt.WorkItemId));
                    }

                    this.EstimatedLocalWorkItemLoad++;
                }
                else
                {
                    var activityInfo = new ActivityInfo()
                    {
                        ActivityId = activityId,
                        IssueTime = evt.Timestamp,
                        OriginWorkItemId = evt.WorkItemId,
                        Message = msg,
                    };

                    if (this.Partition.Settings.ActivityScheduler != ActivitySchedulerOptions.LocalOnly
                        && this.RemotePending.Count < MAX_REMOTE_PENDING)
                    {
                        this.RemotePending.Add((evt.WorkItemId, msg.SequenceNumber), activityInfo);

                        if (!effects.IsReplaying)
                        {
                            this.Send(activityInfo);
                        }
                    }
                    else
                    {
                        this.Backlog.Enqueue(activityInfo);

                        if (!effects.IsReplaying)
                        {
                            this.Partition.WorkItemTraceHelper.TraceTaskMessageReceived(this.Partition.PartitionId, msg, evt.WorkItemId, $"Backlog@{this.Backlog.Count}");

                            //if (this.Backlog.Count == 1)
                            //{
                            //    this.ScheduleNextOffloadDecision(WaitTimeThresholdForOffload);
                            //}
                        }
                    }
                }
            }
        }

        public void Process(LocalActivityCompleted evt, EffectTracker effects)
        {
            this.LocalPending.Remove(evt.ActivityId);
            this.EstimatedLocalWorkItemLoad = evt.ReportedLoad;
            effects.Add(TrackedObjectKey.Sessions);
         
            // now that a local activity has completed, we can perhaps add more from the backlog
            while (this.LocalPending.Count == 0 || this.EstimatedLocalWorkItemLoad <= MAX_LOCAL_PENDING)
            {
                if (this.TryGetNextActivity(out var activityInfo))
                {
                    this.LocalPending.Add(activityInfo.ActivityId, (activityInfo.Message, activityInfo.OriginWorkItemId));

                    if (!effects.IsReplaying)
                    {
                        this.Partition.EnqueueActivityWorkItem(new ActivityLocalWorkItem(this.Partition, activityInfo.ActivityId, activityInfo.Message, activityInfo.OriginWorkItemId));
                    }

                    this.EstimatedLocalWorkItemLoad++;
                }
                else
                {
                    break;
                }
            }
        }

        public void Process(WorkerResultReceived evt, EffectTracker effects)
        {
            if (this.RemotePending.Remove((evt.OriginWorkItemId, evt.OriginSequenceNumber)))
            {
                // process this on sessions to deliver the result to the orchestration              
                effects.Add(TrackedObjectKey.Sessions);

                // now that a remote activity has completed, we can perhaps add more from the backlog
                while (this.RemotePending.Count <= MAX_REMOTE_PENDING)
                {
                    if (this.TryGetNextActivity(out var toSend))
                    {
                        this.RemotePending.Add((toSend.OriginWorkItemId, toSend.Message.SequenceNumber), toSend);

                        if (!effects.IsReplaying)
                        {
                            this.Send(toSend);
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }

        // we are not currently using this. 
        //public void Process(OffloadDecision offloadDecisionEvent, EffectTracker effects)
        //{
        //    // check for offload conditions and if satisfied, send activities to global queue
        //    if (this.Backlog.Count == 0)
        //    {
        //        return; // stop making offload decisions until there is a backlog.
        //    }


        //var 

        //if (offloadDecisionEvent.)


        //    var reportedRemotes = string.Join(",", this.ReportedRemoteLoads.Select((int x, int i) =>
        //        (i == target) ? $"{targetLoad}+{offloadDecisionEvent.OffloadedActivities.Count}" : (x == NOT_CONTACTED ? "-" : (x == RESPONSE_PENDING ? "X" : x.ToString()))));

        //    if (!effects.IsReplaying)
        //    {
        //        this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.LocalPending.Count, this.Backlog.Count, this.QueuedRemotes.Count, reportedRemotes);

        //        // try again relatively soon
        //        this.ScheduleNextOffloadDecision(TimeSpan.FromMilliseconds(200));
        //    }
        //}
        //else
        //{
        //    if (!effects.IsReplaying)
        //    {
        //        this.Partition.EventTraceHelper.TracePartitionOffloadDecision(this.EstimatedLocalWorkItemLoad, this.LocalPending.Count, this.Backlog.Count, this.QueuedRemotes.Count, reportedRemotes);

        //        // there are no eligible recipients... try again in a while
        //        this.ScheduleNextOffloadDecision(TimeSpan.FromSeconds(10));
        //    }
        //}
        //}

        void Send(ActivityInfo activityInfo)
        {
            var workerEvent = new WorkerRequestReceived()
            {
                OriginWorkItemId = activityInfo.OriginWorkItemId,
                Message = activityInfo.Message,
                StartSendTimestamp = this.Partition.CurrentTimeMs,
            };
            DurabilityListeners.Register(workerEvent, this);
            this.Partition.Send(workerEvent);
        }

        public void ConfirmDurable(Event evt)
        {
            var workerRequestReceived = (WorkerRequestReceived)evt;

            this.Partition.SubmitInternalEvent(new WorkerSendConfirmed()
            {
                PartitionId = this.Partition.PartitionId,
                OriginWorkItemId = workerRequestReceived.OriginWorkItemId,
                OriginSequenceNumber = workerRequestReceived.Message.SequenceNumber,
            });

            double sendDelayMs = this.Partition.CurrentTimeMs - workerRequestReceived.StartSendTimestamp;

            this.Partition.WorkItemTraceHelper.TraceTaskMessageSent(this.Partition.PartitionId, workerRequestReceived.Message, workerRequestReceived.OriginWorkItemId, null, sendDelayMs);
        }

        public void Process(WorkerSendConfirmed workerSendConfirmed, EffectTracker effects)
        {
            if (this.RemotePending.TryGetValue((workerSendConfirmed.OriginWorkItemId, workerSendConfirmed.OriginSequenceNumber), out var activityInfo))
            {
                activityInfo.Sent = true;
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
            public bool Sent;
        }
    }
}
