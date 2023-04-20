// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;

    [DataContract]
    class ReassemblyState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, List<PartitionEventFragment>> Fragments { get; private set; } = new Dictionary<string, List<PartitionEventFragment>>();

        [DataMember]
        public bool UseExpirationHorizon { get; set; } 

        [DataMember]
        public DateTime TimeoutHorizon { get; set; }

        [DataMember]
        public Dictionary<uint, (long Position, int SubPosition)> DedupHorizon { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Reassembly);
        public override string ToString()
        {
            return $"Reassembly ({this.Fragments.Count} pending)";
        }

        bool IsExpired(PartitionEventFragment fragment)
        {
            if (fragment.Timeout.HasValue)
            {
                return fragment.Timeout.Value < this.TimeoutHorizon;
            }
            else if (fragment.DedupPosition.HasValue)
            {
                this.DedupHorizon.TryGetValue(fragment.DedupPosition.Value.Item1, out (long, int) lastProcessed);
                return lastProcessed.CompareTo((fragment.DedupPosition.Value.Item2, fragment.DedupPosition.Value.Item3)) >= 0;
            }
            else
            {
                return false;
            }
        }

        public override void Process(RecoveryCompleted evt, EffectTracker effects)
        {
            // set expiration horizon, i.e. a lower limit for the timeout value and receive position of retained fragments
            this.UseExpirationHorizon = evt.UseExpirationHorizonForFragments;
            this.TimeoutHorizon = evt.Timestamp;
            this.DedupHorizon ??= new Dictionary<uint, (long Position, int SubPosition)>();
            foreach (var kvp in evt.ReceivePositions)
            {
                this.DedupHorizon[kvp.Key] = kvp.Value;
            }
           
            // remove expired fragments
            var expired = this.Fragments.Where(kvp => this.IsExpired(kvp.Value.First())).ToList();

            foreach (var kvp in expired)
            {
                this.Fragments.Remove(kvp.Key);

                if (!effects.IsReplaying)
                {
                    effects.EventTraceHelper.TraceEventProcessingDetail($"Dropped {kvp.Value.Count()} expired fragments for id={kvp.Value.First().OriginalEventId} during recovery");
                }
            } 
        }

        public override void Process(PartitionEventFragment evt, EffectTracker effects)
        {            
            if (this.UseExpirationHorizon && this.IsExpired(evt))
            {
                return; // we now entirely ignore expired fragments (fixes issue described in #253)
            }

            // stores fragments until the last one is received
            var group = evt.GroupId.HasValue 
                ? evt.GroupId.Value.ToString()       // groups are now the way we track fragments
                : evt.OriginalEventId.ToString();  // prior to introducing groups, we used just the event id, which is not correct under interleavings

            try
            {
                if (evt.IsLast)
                {
                    evt.ReassembledEvent = FragmentationAndReassembly.Reassemble<PartitionEvent>(this.Fragments[group], evt, effects.Partition);

                    effects.EventDetailTracer?.TraceEventProcessingDetail($"Reassembled {evt.ReassembledEvent}");

                    this.Fragments.Remove(group);

                    switch (evt.ReassembledEvent)
                    {
                        case PartitionUpdateEvent updateEvent:
                            if (!effects.IsReplaying)
                            {
                                updateEvent.OnSubmit(this.Partition);
                            }
                            updateEvent.DetermineEffects(effects);
                            break;

                        case PartitionReadEvent readEvent:
                            this.Partition.SubmitEvent(readEvent);
                            break;

                        case PartitionQueryEvent queryEvent:
                            this.Partition.SubmitParallelEvent(queryEvent);
                            break;

                        default:
                            throw new InvalidCastException("Could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                    }
                }
                else
                {
                    List<PartitionEventFragment> list;

                    if (evt.Fragment == 0)
                    {
                        this.Fragments[group] = list = new List<PartitionEventFragment>();
                    }
                    else
                    {
                        list = this.Fragments[group];
                    }

                    list.Add(evt);
                }
            }
            catch (System.NullReferenceException e) when (!this.UseExpirationHorizon)
            {
                // An earlier version of this code was throwing NullReferenceException (see #235).
                // This was fixed by introducing the expiration horizon. However, when replaying event
                // logs from before this check was introduced, that feature does not take effect yet
                // (it is activated only after the log is replayed, when processing RecoveryCompletedEvent) and
                // the exception is still thrown. To allow recovery of task hubs stuck in this situation, we catch it.
                effects.EventTraceHelper.TraceEventProcessingWarning($"Ignored NullReferenceException while processing {evt} id={evt.EventIdString} : {e}");
            }
        }
    }
}
