// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
    class PartitionEventFragment : 
        PartitionUpdateEvent, 
        FragmentationAndReassembly.IEventFragment
    {
        [DataMember]
        public EventId OriginalEventId { get; set; }

        [DataMember]
        public byte[] Bytes { get; set; }

        [DataMember]
        public int Fragment { get; set; }

        [DataMember]
        public bool IsLast { get; set; }

        [IgnoreDataMember]
        public PartitionEvent ReassembledEvent;

        public override EventId EventId => EventId.MakeSubEventId(this.OriginalEventId, this.Fragment);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.Bytes.Length);
            if (this.IsLast)
            {
                s.Append(" last");
            }
        }


        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Reassembly);
        }
    }
}