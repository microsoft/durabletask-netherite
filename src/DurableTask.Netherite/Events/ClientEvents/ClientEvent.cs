// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
