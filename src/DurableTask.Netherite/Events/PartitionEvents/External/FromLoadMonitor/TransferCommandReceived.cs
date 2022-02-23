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
    class TransferCommandReceived : PartitionMessageEvent
    {
        [DataMember]
        public Guid RequestId { get; set; }

        [DataMember]
        public int NumActivitiesToSend { get; set; }

        [DataMember]
        public uint TransferDestination { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public List<(TaskMessage, string)> TransferredActivities { get; set; }

        public override EventId EventId => EventId.MakeLoadMonitorToPartitionEventId(this.RequestId, this.PartitionId);

        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages => throw new NotImplementedException();

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
           base.ExtraTraceInformation(s);
           s.Append(' ');
           s.Append(this.NumActivitiesToSend);
           s.Append("->Part");
           s.Append(this.TransferDestination.ToString("D2"));     
        }
    }
}