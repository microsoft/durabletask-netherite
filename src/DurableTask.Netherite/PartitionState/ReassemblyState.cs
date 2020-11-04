// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.Serialization;

    [DataContract]
    class ReassemblyState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, List<PartitionEventFragment>> Fragments { get; private set; } = new Dictionary<string, List<PartitionEventFragment>>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Reassembly);
        public override string ToString()
        {
            return $"Reassembly ({this.Fragments.Count} pending)";
        }

        public override void Process(PartitionEventFragment evt, EffectTracker effects)
        {
            // stores fragments until the last one is received
            var originalEventString = evt.OriginalEventId.ToString();
            if (evt.IsLast)
            {
                evt.ReassembledEvent =  FragmentationAndReassembly.Reassemble<PartitionEvent>(this.Fragments[originalEventString], evt);
                
                this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Reassembled {evt.ReassembledEvent}");

                this.Fragments.Remove(originalEventString);

                switch (evt.ReassembledEvent)
                {
                    case PartitionUpdateEvent updateEvent:
                        updateEvent.DetermineEffects(effects);
                        break;

                    case PartitionReadEvent readEvent:
                        this.Partition.SubmitInternalEvent(readEvent);
                        break;

                    case PartitionQueryEvent queryEvent:
                        this.Partition.SubmitInternalEvent(queryEvent);
                        break;

                    default:
                        throw new InvalidCastException("Could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                }
            }
            else
            {
                if (!this.Fragments.TryGetValue(originalEventString, out var list))
                {
                    this.Fragments[originalEventString] = list = new List<PartitionEventFragment>();
                }
                list.Add(evt);
            }
        } 
    }
}
