// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
class WaitResponseReceived : ClientEvent
    {
        [DataMember]
        public OrchestrationState OrchestrationState { get; set; }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.OrchestrationState.OrchestrationStatus.ToString());
        }
    }
}
