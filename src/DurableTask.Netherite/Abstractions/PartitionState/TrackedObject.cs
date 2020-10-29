//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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

        /// <summary>
        /// The current value in serialized form, or null
        /// </summary>
        [IgnoreDataMember]
        internal byte[] SerializationCache { get; set; }

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
        /// Is called on all singleton objects once at the very beginning
        /// </summary>
        public virtual void OnFirstInitialization()
        {
        }

        /// <summary>
        /// Is automatically called on all singleton objects after recovery. Typically used to
        /// restart pending activities, timers, tasks and the like.
        /// </summary>
        public virtual void OnRecoveryCompleted()
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
            dynamic dynamicThis = this;
            dynamic dynamicPartitionEvent = e.ReassembledEvent;
            dynamicThis.Process(dynamicPartitionEvent, effects);
        }
    }
}
