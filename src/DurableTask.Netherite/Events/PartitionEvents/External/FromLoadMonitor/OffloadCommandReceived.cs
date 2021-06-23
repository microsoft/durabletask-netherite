// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class OffloadCommandReceived : PartitionMessageEvent
    {
        [DataMember]
        public Guid RequestId { get; set; }

        [DataMember]
        public int NumActivitiesToSend { get; set; }

        [DataMember]
        public uint OffloadDestination { get; set; }

        [IgnoreDataMember]
        public List<(TaskMessage, string)> OffloadedActivities { get; set; }

        public override EventId EventId => EventId.MakeLoadMonitorToPartitionEventId(this.RequestId, this.PartitionId);

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }
    }
}