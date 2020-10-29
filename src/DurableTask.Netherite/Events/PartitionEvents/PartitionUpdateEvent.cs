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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
abstract class PartitionUpdateEvent : PartitionEvent
    {
        /// <summary>
        /// The position of the next event after this one. For read-only events, zero.
        /// </summary>
        /// <remarks>We do not persist this in the log since it is implicit, nor transmit this in packets since it has only local meaning.</remarks>
        [IgnoreDataMember]
        public long NextCommitLogPosition { get; set; }

        [IgnoreDataMember]
        public OutboxState.Batch OutboxBatch { get; set; }

        [IgnoreDataMember]
        public virtual IEnumerable<TaskMessage> TracedTaskMessages => PartitionUpdateEvent.noTaskMessages;

        static readonly IEnumerable<TaskMessage> noTaskMessages = Enumerable.Empty<TaskMessage>();

        public abstract void DetermineEffects(EffectTracker effects);
    }
}