// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
