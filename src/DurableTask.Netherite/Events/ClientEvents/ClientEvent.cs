// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    abstract class ClientEvent : Event
    {
        [DataMember]
        public Guid ClientId { get; set; }

        [DataMember]
        public long RequestId { get; set; }

        public override EventId EventId => EventId.MakeClientResponseEventId(this.ClientId, this.RequestId);
    }
}
