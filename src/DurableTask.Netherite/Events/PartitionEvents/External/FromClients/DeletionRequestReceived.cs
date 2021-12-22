// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
    class DeletionRequestReceived : ClientRequestEventWithPrefetch
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public DateTime? CreatedTime { get; set; }  // works like an e-tag, if specified

        [IgnoreDataMember]
        public override TrackedObjectKey Target => TrackedObjectKey.Instance(this.InstanceId);

        [IgnoreDataMember]
        public override TrackedObjectKey? Prefetch => TrackedObjectKey.History(this.InstanceId);

        [IgnoreDataMember]
        public override string TracedInstanceId => this.InstanceId;

        [IgnoreDataMember]
        public DeletionResponseReceived ResponseToSend { get; set; } // used to communicate response to ClientState
    }
}
