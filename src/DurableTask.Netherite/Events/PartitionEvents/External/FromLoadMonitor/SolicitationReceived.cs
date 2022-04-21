// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class SolicitationReceived : PartitionMessageEvent
    {
        [DataMember]
        public Guid RequestId { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        public override EventId EventId => EventId.MakeLoadMonitorToPartitionEventId(this.RequestId, this.PartitionId);

        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages => throw new NotImplementedException();

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }

        [IgnoreDataMember]
        public override bool CountsAsPartitionActivity => false;
        
        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }
    }
}