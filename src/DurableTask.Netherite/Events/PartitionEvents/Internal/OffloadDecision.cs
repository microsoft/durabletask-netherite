// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        public SortedDictionary<uint, List<(TaskMessage,string)>> ActivitiesToTransfer { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => 
            EventId.MakePartitionInternalEventId($"{this.PartitionId:D2}F{this.Timestamp:o}");

        public override void DetermineEffects(EffectTracker effects)
        {
            // start processing on activities, which makes the decision, 
            // and if offloading, fills in the fields, and adds the outbox to the effects
            effects.Add(TrackedObjectKey.Activities);
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }
    }
}
