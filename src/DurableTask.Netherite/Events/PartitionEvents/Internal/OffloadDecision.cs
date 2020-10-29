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

    [DataContract]
class OffloadDecision : PartitionUpdateEvent
    {
        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public uint DestinationPartitionId { get; set; }

        [IgnoreDataMember]
        public List<TaskMessage> OffloadedActivities { get; set; }

        public static string GetWorkItemId(uint partition, DateTime timestamp) => $"{partition:D2}-O{timestamp:o}";

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(GetWorkItemId(this.PartitionId, this.Timestamp));

        public override void DetermineEffects(EffectTracker effects)
        {
            // start processing on activities, which makes the decision, 
            // and if offloading, fills in the fields, and adds the outbox to the effects
            effects.Add(TrackedObjectKey.Activities);
        }
    }
}
