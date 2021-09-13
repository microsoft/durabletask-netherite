// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
    abstract class ClientRequestEventWithPrefetch : ClientRequestEvent, IClientRequestEvent, IRequiresPrefetch
    {
        [DataMember]
        public ProcessingPhase Phase { get; set; }

        [IgnoreDataMember]
        public abstract TrackedObjectKey Target { get; }

        public virtual bool OnReadComplete(TrackedObject target, Partition partition)
        {
            return true;
        }

        [IgnoreDataMember]
        public virtual TrackedObjectKey? Prefetch => null;

        IEnumerable<TrackedObjectKey> IRequiresPrefetch.KeysToPrefetch
        {
            get
            {
                yield return this.Target;
                var secondPrefetch = this.Prefetch;
                if (secondPrefetch.HasValue)
                {
                    yield return secondPrefetch.Value;
                }
            }
        }
        public override void OnSubmit(Partition partition)
        {
            if (this.Phase == ProcessingPhase.Read)
            {
                partition.SubmitEvent(new PrefetchState.InstancePrefetch(this));
            }
        }

        public sealed override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Prefetch);
        }

        public enum ProcessingPhase
        { 
             Read,
             Confirm,
             ConfirmAndProcess,
        }
    }
}