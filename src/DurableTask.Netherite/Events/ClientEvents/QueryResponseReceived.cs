// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class QueryResponseReceived : ClientEvent
    {
        [DataMember]
        public List<OrchestrationState> OrchestrationStates { get; set; }

        [DataMember]
        public int? Final { get; set; }

        public override string ToString() => $"Count={this.OrchestrationStates.Count} Final={this.Final}";
    }
}
