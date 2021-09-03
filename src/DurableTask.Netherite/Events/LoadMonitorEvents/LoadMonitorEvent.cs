// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    abstract class LoadMonitorEvent : Event
    {
        [DataMember]
        public Guid RequestId { get; set; }

        public override EventId EventId => EventId.MakeLoadMonitorEventId(this.RequestId);
    }
}
