﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using DurableTask.Netherite.Scaling;
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    /// <summary>
    /// An object whose value is persisted by storage, and that is indexed by a primary key.
    /// </summary>
    [DataContract]
    [KnownTypeAttribute("KnownTypes")]
    abstract class TrackedObject
    {
        /// <summary>
        /// The partition to which this object belongs.
        /// </summary>
        [IgnoreDataMember]
        public Partition Partition;

        /// <summary>
        /// The key for this object.
        /// </summary>
        [IgnoreDataMember]
        public abstract TrackedObjectKey Key { get; }

        [DataMember]
        public int Version { get; set;  } // we use this validate consistency of read/write updates in FASTER, it is not otherwise needed

        [IgnoreDataMember]
        public long LastUpdate { get; set; } // workaround for filtering out occasional duplicate RMW invocations (#236)

        /// <summary>
        /// The collection of all types of tracked objects and polymorphic members of tracked objects. Can be
        /// used by serializers to compute a type map.
        /// </summary>
        /// <returns>The collection of types.</returns>
        static IEnumerable<Type> KnownTypes()
        {
            foreach (var t in Core.History.HistoryEvent.KnownTypes())
            {
                yield return t;
            }
            foreach (var t in DurableTask.Netherite.Event.KnownTypes())
            {
                yield return t;
            }
            foreach (var t in TrackedObjectKey.TypeMap.Values)
            {
                yield return t;
            }
        }

        /// <summary>
        /// An estimation of the size that this object takes up in memory
        /// </summary>
        public virtual long EstimatedSize => 0;

        /// <summary>
        /// Is called on all singleton objects once at the very beginning
        /// </summary>
        public virtual void OnFirstInitialization(Partition partition)
        {
        }

        /// <summary>
        /// Is called to update the load information that is published
        /// </summary>
        /// <param name="info"></param>
        public virtual void UpdateLoadInfo(PartitionLoadInfo info)
        {
        }

        public virtual void Process(PartitionEventFragment e, EffectTracker effects)
        {
            // processing a reassembled event just applies the original event
            ((PartitionUpdateEvent) e.ReassembledEvent).ApplyTo(this, effects);  
        }

        public virtual void Process(BatchProcessed evt, EffectTracker tracker) { }
        public virtual void Process(CreationRequestReceived evt, EffectTracker tracker) { }
        public virtual void Process(DeletionRequestReceived evt, EffectTracker tracker) { }
        public virtual void Process(InstanceQueryReceived evt, EffectTracker tracker) { }
        public virtual void Process(PurgeRequestReceived evt, EffectTracker tracker) { }
        public virtual void Process(WaitRequestReceived evt, EffectTracker tracker) { }
        public virtual void Process(PurgeBatchIssued evt, EffectTracker tracker) { }
        public virtual void Process(ClientTaskMessagesReceived evt, EffectTracker tracker) { }
        public virtual void Process(AcksReceived evt, EffectTracker tracker) { }
        public virtual void Process(SolicitationReceived evt, EffectTracker tracker) { }
        public virtual void Process(TransferCommandReceived evt, EffectTracker tracker) { }
        public virtual void Process(ActivityTransferReceived evt, EffectTracker tracker) { }
        public virtual void Process(RemoteActivityResultReceived evt, EffectTracker tracker) { }
        public virtual void Process(TaskMessagesReceived evt, EffectTracker tracker) { }
        public virtual void Process(ActivityCompleted evt, EffectTracker tracker) { }
        public virtual void Process(OffloadDecision evt, EffectTracker tracker) { }
        public virtual void Process(SendConfirmed evt, EffectTracker tracker) { }
        public virtual void Process(TimerFired evt, EffectTracker tracker) { }
        public virtual void Process(RecoveryCompleted evt, EffectTracker tracker) { }
    }
}
