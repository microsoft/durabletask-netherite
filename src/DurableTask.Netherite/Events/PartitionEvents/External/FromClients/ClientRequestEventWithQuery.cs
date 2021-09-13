// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

        [DataMember]
        public string ContinuationToken { get; set; }

        [DataMember]
        public int PageSize { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeClientRequestEventId(this.ClientId, this.RequestId);

        public override void OnSubmit(Partition partition)
        {
            if (this.Phase == ProcessingPhase.Query)
            {
                partition.SubmitParallelEvent(new QueriesState.InstanceQueryEvent(this));
            }
        }

        public abstract Task OnQueryCompleteAsync(
            IAsyncEnumerable<OrchestrationState> result, 
            Partition partition,
            PartitionQueryEvent evt);

        public sealed override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Queries);
        }

        public enum ProcessingPhase
        { 
             Query,
             Confirm,
        }
    }
}