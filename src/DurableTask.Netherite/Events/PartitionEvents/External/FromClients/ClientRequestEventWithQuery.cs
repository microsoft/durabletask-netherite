// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
abstract class ClientRequestEventWithQuery : ClientRequestEvent, IClientRequestEvent
    {
        [DataMember]
        public ProcessingPhase Phase { get; set; }
       
        [DataMember]
        public InstanceQuery InstanceQuery { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeClientRequestEventId(this.ClientId, this.RequestId);

        public abstract Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Partition partition);

        public sealed override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Queries);
        }

        public enum ProcessingPhase
        { 
             Query,
             Confirm,
             ConfirmAndProcess,
        }
    }
}