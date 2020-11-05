// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;

    [DataContract]
class SendConfirmed : PartitionUpdateEvent
    {
        [DataMember]
        public long Position { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => $"{this.PartitionId:D2}C{this.Position:D10}";

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Outbox);
        }
    }
}
