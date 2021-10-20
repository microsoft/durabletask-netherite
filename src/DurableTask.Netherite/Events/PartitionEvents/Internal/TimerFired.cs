// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class TimerFired : PartitionUpdateEvent
    {
        [DataMember]
        public long TimerId { get; set; }
        
        [DataMember]
        public DateTime Due { get; set; }

        [DataMember]
        public TaskMessage TaskMessage { get; set; }

        [DataMember]
        public string OriginWorkItemId { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => $"{this.PartitionId:D2}T{this.TimerId}";

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);

        [IgnoreDataMember]
        public override string TracedInstanceId => this.TaskMessage.OrchestrationInstance.InstanceId;


        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Sessions);
            effects.Add(TrackedObjectKey.Timers);
        }
    }
}
