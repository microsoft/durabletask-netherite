﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using System;
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

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public WaitResponseReceived ResponseToSend { get; set; } // used to communicate response to ClientState

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public static bool SatisfiesWaitCondition(OrchestrationStatus? value)
             => (value.HasValue &&
                 value.Value != OrchestrationStatus.Running &&
                 value.Value != OrchestrationStatus.Pending &&
                 value.Value != OrchestrationStatus.ContinuedAsNew);

        public WaitResponseReceived CreateResponse(OrchestrationState value)
            => new WaitResponseReceived()
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationState = value
            };
    }
}
