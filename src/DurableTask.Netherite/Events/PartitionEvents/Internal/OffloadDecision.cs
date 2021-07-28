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

        public static string GetWorkItemId(uint partition, DateTime timestamp) => $"{partition:D2}F{timestamp:o}";

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(GetWorkItemId(this.PartitionId, this.Timestamp));

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }
    }
}
