// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }
    }
}
