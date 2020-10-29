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
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    [DataContract]
class RemoteActivityResultReceived : PartitionMessageEvent
    {
        [DataMember]
        public TaskMessage Result { get; set; }

        [DataMember]
        public long ActivityId { get; set; }

        [DataMember]
        public int ActivitiesQueueSize { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(ActivitiesState.GetWorkItemId(this.OriginPartition, this.ActivityId), this.PartitionId);

        [IgnoreDataMember]
        public override IEnumerable<TaskMessage> TracedTaskMessages { get { yield return this.Result; } }
    }
}