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

        [DataMember]
        public string ContinuationToken { get; set; }

        [IgnoreDataMember]
        public bool NonEmpty => this.OrchestrationStates.Count > 0 || this.ContinuationToken != null;

        public override string ToString() => $"Count: {this.OrchestrationStates.Count}";
    }
}
