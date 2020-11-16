// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core.History;

    [DataContract]
    class HistoryResponseReceived : ClientEvent
    {
        [DataMember]
        public string ExecutionId { get; set; }

        [DataMember]
        public List<HistoryEvent> History { get; set; }
    }
}
