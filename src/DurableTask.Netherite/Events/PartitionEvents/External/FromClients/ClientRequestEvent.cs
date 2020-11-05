// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
abstract class ClientRequestEvent : PartitionUpdateEvent, IClientRequestEvent
    {
        [DataMember]
        public Guid ClientId { get; set; }

        [DataMember]
        public long RequestId { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => WorkItemTraceHelper.FormatClientWorkItemId(this.ClientId, this.RequestId);

        [DataMember]
        public DateTime TimeoutUtc { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeClientRequestEventId(this.ClientId, this.RequestId);
    }
}