// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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