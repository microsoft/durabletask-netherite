// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
    class AcksReceived : PartitionMessageEvent
    {
        [DataMember]
        public Guid RequestId { get; set; }

        [DataMember]
        public (long, int)?[] ReceivePositions { get; set; } // for each partition

        public override EventId EventId => EventId.MakeLoadMonitorToPartitionEventId(this.RequestId, this.PartitionId);

        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages => throw new NotImplementedException(); // never in outbox

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Outbox);
        }

        [IgnoreDataMember]
        public override bool CountsAsPartitionActivity => false;

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }
    }
}