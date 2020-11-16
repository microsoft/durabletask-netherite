// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
    class HistoryRequestReceived : ClientReadonlyRequestEvent
    {
        [DataMember]
        public string InstanceId { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey ReadTarget => TrackedObjectKey.History(this.InstanceId);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public override void OnReadComplete(TrackedObject target, Partition partition)
        {
            var historyState = (HistoryState)target;

            var response = new HistoryResponseReceived()
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                ExecutionId = historyState?.ExecutionId,
                History = historyState?.History?.ToList(),
            };

            partition.Send(response);
        }
    }
}
