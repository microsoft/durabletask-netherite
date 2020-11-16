// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class ClientTaskMessagesReceived : ClientRequestEvent
    {
        [DataMember]
        public TaskMessage[] TaskMessages { get; set; }

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage, string)> TracedTaskMessages
        {
            get
            {
                foreach (var message in this.TaskMessages)
                {
                    yield return (message, this.WorkItemId);
                }
            }
        }


        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Sessions);
        }
    }
}