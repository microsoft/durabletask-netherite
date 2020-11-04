// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
