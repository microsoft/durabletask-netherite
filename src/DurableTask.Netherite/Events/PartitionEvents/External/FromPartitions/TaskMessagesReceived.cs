//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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