// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
class StateRequestReceived : ClientReadonlyRequestEvent
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public bool IncludeInput { get; set; }

        [DataMember]
        public bool IncludeOutput { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey ReadTarget => TrackedObjectKey.Instance(this.InstanceId);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public override void OnReadComplete(TrackedObject target, Partition partition)
        {
            var orchestrationState = ((InstanceState)target)?.OrchestrationState;
            var editedState = orchestrationState?.ClearFieldsImmutably(this.IncludeInput, this.IncludeOutput);

            var response = new StateResponseReceived()
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationState = editedState,
            };

            partition.Send(response);
        }
    }
}
