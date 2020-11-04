// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
class WaitRequestReceived : ClientRequestEventWithPrefetch
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        public override TrackedObjectKey Target => TrackedObjectKey.Instance(this.InstanceId);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public static bool SatisfiesWaitCondition(OrchestrationState value)
             => (value != null &&
                 value.OrchestrationStatus != OrchestrationStatus.Running &&
                 value.OrchestrationStatus != OrchestrationStatus.Pending &&
                 value.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew);

        public WaitResponseReceived CreateResponse(OrchestrationState value)
            => new WaitResponseReceived()
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationState = value
            };
    }
}
