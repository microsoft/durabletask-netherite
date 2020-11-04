// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    [DataContract]
class RemoteActivityResultReceived : PartitionMessageEvent
    {
        [DataMember]
        public TaskMessage Result { get; set; }

        [DataMember]
        public long ActivityId { get; set; }

        [DataMember]
        public int ActivitiesQueueSize { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(ActivitiesState.GetWorkItemId(this.OriginPartition, this.ActivityId), this.PartitionId);

        [IgnoreDataMember]
        public override IEnumerable<TaskMessage> TracedTaskMessages { get { yield return this.Result; } }
    }
}