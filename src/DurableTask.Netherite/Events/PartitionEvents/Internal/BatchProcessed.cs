// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Core.History;

    [DataContract]
    class BatchProcessed : PartitionUpdateEvent
    {
        [DataMember]
        public long SessionId { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public long BatchStartPosition { get; set; }

        [DataMember]
        public int BatchLength { get; set; }

        [DataMember]
        public List<HistoryEvent> NewEvents { get; set; }

        [DataMember]
        public OrchestrationState State { get; set; }

        [DataMember]
        public List<TaskMessage> ActivityMessages { get; set; }

        [DataMember]
        public List<TaskMessage> LocalMessages { get; set; }

        [DataMember]
        public List<TaskMessage> RemoteMessages { get; set; }

        [DataMember]
        public List<TaskMessage> TimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public bool IsPersisted { get; set; }

        [IgnoreDataMember]
        public OrchestrationWorkItem WorkItemForReuse { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => SessionsState.GetWorkItemId(this.PartitionId, this.SessionId, this.BatchStartPosition);

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.IsPersisted ? this.WorkItemId + "P" : this.WorkItemId);

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage,string)> TracedTaskMessages 
        { 
            get
            {
                string workItemId = SessionsState.GetWorkItemId(this.PartitionId, this.SessionId, this.BatchStartPosition);
                if (this.ActivityMessages != null)
                {
                    foreach (TaskMessage a in this.ActivityMessages)
                    {
                        yield return (a, workItemId);
                    }
                }
                if (this.TimerMessages != null)
                {
                    foreach (TaskMessage t in this.TimerMessages)
                    {
                        yield return (t, workItemId);
                    }
                }
                if (this.LocalMessages != null)
                {
                    foreach (TaskMessage l in this.LocalMessages)
                    {
                        yield return (l, workItemId);
                    }
                }
                if (this.RemoteMessages != null)
                {
                    foreach (TaskMessage r in this.RemoteMessages)
                    {
                        yield return (r, workItemId);
                    }
                }
            }
        }

        public override void DetermineEffects(EffectTracker effects)
        {
            // start on the sessions object; further effects are determined from there
            effects.Add(TrackedObjectKey.Sessions);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            base.ExtraTraceInformation(s);

            if (this.State != null)
            {
                s.Append(' ');
                s.Append(this.State.OrchestrationStatus);
            }

            s.Append(' ');
            s.Append(this.InstanceId);
        }
    }
}
