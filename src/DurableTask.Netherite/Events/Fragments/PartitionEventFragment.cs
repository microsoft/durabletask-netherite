// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
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
        public Guid? GroupId { get; set; } // we now use a group id for tracking fragments, to fix issue #231

        [DataMember]
        public byte[] Bytes { get; set; }

        [DataMember]
        public int Fragment { get; set; }

        [DataMember]
        public bool IsLast { get; set; }

        [DataMember]
        public DateTime? Timeout { get; set; } // so we can remove incomplete fragments from client requests after they time out

        [DataMember]
        public (uint,long,int)? DedupPosition { get; set; } // so we can remove incomplete fragments from other partitions

        [IgnoreDataMember]
        public PartitionEvent ReassembledEvent;

        public override EventId EventId => EventId.MakeSubEventId(this.OriginalEventId, this.Fragment);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            if (this.GroupId.HasValue)
            {
                s.Append(this.GroupId.Value.ToString("N"));
                s.Append(' ');
            }
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

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

    }
}