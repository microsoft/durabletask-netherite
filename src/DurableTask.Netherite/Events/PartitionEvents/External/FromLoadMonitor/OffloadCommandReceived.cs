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
    class OffloadCommandReceived : PartitionMessageEvent
    {
        [DataMember]
        public Guid RequestId { get; set; }

        [DataMember]
        public int NumActivitiesToSend { get; set; }

        [DataMember]
        public uint OffloadDestination { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public List<(TaskMessage, string)> OffloadedActivities { get; set; }

        public override EventId EventId => EventId.MakeLoadMonitorToPartitionEventId(this.RequestId, this.PartitionId);

        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages => throw new NotImplementedException();

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
           base.ExtraTraceInformation(s);

           s.Append(this.NumActivitiesToSend);
           s.Append("->Part");
           s.Append(this.OffloadDestination.ToString("D2"));     
        }
    }
}