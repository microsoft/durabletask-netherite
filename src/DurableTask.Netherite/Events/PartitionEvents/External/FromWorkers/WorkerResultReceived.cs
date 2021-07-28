// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class WorkerResultReceived : PartitionUpdateEvent
    {
        [DataMember]
        public TaskMessage Result { get; set; }

        [DataMember]
        public string OriginWorkItemId { get; set; }

        [DataMember]
        public long OriginSequenceNumber { get; set; }

        [DataMember]
        public Guid WorkerId { get; set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeWorkerResponseEventId(this.WorkerId, this.SequenceNumber);

        public override void DetermineEffects(EffectTracker effects)
        {
            // start on activities, will be handed off to sessions if not duplicate or squashed
            effects.Add(TrackedObjectKey.Activities);
        }
    }
}
