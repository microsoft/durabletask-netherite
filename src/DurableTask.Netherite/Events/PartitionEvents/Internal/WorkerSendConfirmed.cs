// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    class WorkerSendConfirmed : PartitionUpdateEvent
    {
        [DataMember]
        public string OriginWorkItemId { get; set; }

        [DataMember]
        public long OriginSequenceNumber { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => $"{this.OriginWorkItemId}M{this.OriginSequenceNumber}C";

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }
    }
}
