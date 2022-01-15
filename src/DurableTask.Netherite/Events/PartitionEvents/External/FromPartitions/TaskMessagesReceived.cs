// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
    class TaskMessagesReceived : PartitionMessageEvent
    {
        [DataMember]
        public List<TaskMessage> TaskMessages { get; set; }

        [DataMember]
        public List<TaskMessage> DelayedTaskMessages { get; set; }

        [DataMember]
        public int SubPosition { get; set; }

        [DataMember]
        public string WorkItemId { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(this.WorkItemId, this.PartitionId);

        [IgnoreDataMember]
        public override (long, int) DedupPosition => (this.OriginPosition, this.SubPosition);

        [IgnoreDataMember]
        public int NumberMessages => (this.TaskMessages?.Count ?? 0) + (this.DelayedTaskMessages?.Count ?? 0);

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages
        {
            get
            {
                if (this.TaskMessages?.Count > 0)
                {
                    foreach (var taskMessage in this.TaskMessages)
                    {
                        yield return (taskMessage, this.WorkItemId);
                    }
                }
                if (this.DelayedTaskMessages?.Count > 0)
                {
                    foreach (var taskMessage in this.DelayedTaskMessages)
                    {
                        yield return (taskMessage, this.WorkItemId);
                    }
                }
            }
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            var tCount = this.TaskMessages?.Count ?? 0;
            var dCount = this.DelayedTaskMessages?.Count ?? 0;

            s.Append(' ');
            if (tCount == 1)
            {
                s.Append(this.TaskMessages[0].Event.EventType);
            }
            else if (dCount == 1)
            {
                s.Append(this.DelayedTaskMessages[0].Event.EventType);
            }
            else
            {
                s.Append('[');
                s.Append(tCount + dCount);
                s.Append(']');
            }
        }
    }
}