// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    class PersistenceConfirmationEvent : PartitionMessageEvent
    {
        // TODO: Implement this
        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(string.Format("pers-conf-{0}-{1}", this.OriginPartition, this.OriginPosition), this.PartitionId);
    }
}