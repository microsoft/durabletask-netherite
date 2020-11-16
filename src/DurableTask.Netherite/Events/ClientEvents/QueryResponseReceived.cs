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
        public IList<OrchestrationState> OrchestrationStates { get; set; }

        public override string ToString() => $"Count: {this.OrchestrationStates.Count}";
    }
}
