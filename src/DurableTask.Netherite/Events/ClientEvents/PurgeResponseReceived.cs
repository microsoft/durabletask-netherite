// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
    class PurgeResponseReceived : ClientEvent
    {
        [DataMember]
        public int NumberInstancesPurged { get; set; }

        [DataMember]
        public string ContinuationToken { get; set; }  // null indicates we have reached the end of all instances in this partition

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(" count=");
            s.Append(this.NumberInstancesPurged);
        }
    }
}
