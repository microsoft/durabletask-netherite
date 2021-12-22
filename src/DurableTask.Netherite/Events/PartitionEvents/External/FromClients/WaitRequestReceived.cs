// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

        [IgnoreDataMember]
        public override TrackedObjectKey Target => TrackedObjectKey.Instance(this.InstanceId);

        [IgnoreDataMember]
        public override string TracedInstanceId => this.InstanceId;

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

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
