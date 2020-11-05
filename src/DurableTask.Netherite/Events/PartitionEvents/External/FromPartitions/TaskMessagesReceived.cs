// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        public string WorkItemId { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(this.WorkItemId, this.PartitionId);

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