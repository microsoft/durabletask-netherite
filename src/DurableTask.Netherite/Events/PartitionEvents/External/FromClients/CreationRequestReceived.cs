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
    using DurableTask.Core;
    using DurableTask.Core.History;

    [DataContract]
class CreationRequestReceived : ClientRequestEventWithPrefetch
    {
        [DataMember]
        public OrchestrationStatus[] DedupeStatuses { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public TaskMessage TaskMessage { get; set; }
        
        [DataMember]
        public bool FilteredDuplicate { get; set; }

        [DataMember]
        public OrchestrationStatus? ExistingInstanceOrchestrationStatus { get; set; }

        [IgnoreDataMember]
        public ExecutionStartedEvent ExecutionStartedEvent => this.TaskMessage.Event as ExecutionStartedEvent;

        [IgnoreDataMember]
        public string InstanceId => this.ExecutionStartedEvent.OrchestrationInstance.InstanceId;

        [IgnoreDataMember]
        public override IEnumerable<TaskMessage> TracedTaskMessages { get { yield return this.TaskMessage; } }

        [IgnoreDataMember]
        public override TrackedObjectKey Target => TrackedObjectKey.Instance(this.InstanceId);


        public override bool OnReadComplete(TrackedObject target, Partition partition)
        {
            // Use this moment of time as the creation timestamp, replacing the original timestamp taken on the client.
            // This is preferrable because it avoids clock synchronization issues (which can result in negative orchestration durations)
            // and means the timestamp is consistently ordered with respect to timestamps of other events on this partition.
            DateTime creationTimestamp = DateTime.UtcNow;

            this.ExecutionStartedEvent.Timestamp = creationTimestamp;

            return true;
        }
    }
}