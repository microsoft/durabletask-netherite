// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;

    [DataContract]
    class PurgeBatchIssued : PartitionUpdateEvent, IRequiresPrefetch, TransportAbstraction.IDurabilityListener
    {
        [DataMember]
        public string QueryEventId { get; set; }

        [DataMember]
        public int BatchNumber { get; set; }

        [DataMember]
        public List<string> InstanceIds { get; set; }

        [DataMember]
        public InstanceQuery InstanceQuery { get; set; }

        [IgnoreDataMember]
        public TaskCompletionSource<object> WhenProcessed { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.QueryEventId);

        [IgnoreDataMember]
        public List<string> Purged { get; set; }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(" batchNumber=");
            s.Append(this.BatchNumber);
            s.Append(" count=");
            s.Append(this.InstanceIds.Count);
        }

        public IEnumerable<TrackedObjectKey> KeysToPrefetch
        {
            get
            {
                foreach (var instanceId in this.InstanceIds)
                {
                    yield return TrackedObjectKey.Instance(instanceId);
                }
            }
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        public override void DetermineEffects(EffectTracker effects)
        {
            // the following singletons are added first, so they are processed last, and can thus
            // access the Purged list that contains only the instance ids that were actually purged

            effects.Add(TrackedObjectKey.Stats);
            effects.Add(TrackedObjectKey.Queries);
            effects.Add(TrackedObjectKey.Sessions);

            this.Purged = new List<string>();
            foreach (string instanceId in this.InstanceIds)
            {
                effects.Add(TrackedObjectKey.Instance(instanceId));
            }
        }

        public void ConfirmDurable(Event evt)
        {
            // lets the client know this batch has been persisted
            this.WhenProcessed.TrySetResult(null);
        }
    }
}