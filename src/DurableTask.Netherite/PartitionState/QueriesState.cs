// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Scaling;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;

    [DataContract]
    class QueriesState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, ClientRequestEventWithQuery> PendingQueries { get; private set; } = new Dictionary<string, ClientRequestEventWithQuery>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Queries);

        public override void OnRecoveryCompleted()
        {
            // reissue queries that did not complete prior to crash/recovery
            foreach (var kvp in this.PendingQueries)
            {
                this.Partition.SubmitInternalEvent(new InstanceQueryEvent(kvp.Value));
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Requests += this.PendingQueries.Count;
        }

        public override string ToString()
        {
            return $"Queries ({this.PendingQueries.Count} pending)";
        }

        public void Process(ClientRequestEventWithQuery clientRequestEvent, EffectTracker effects)
        {
            if (clientRequestEvent.Phase == ClientRequestEventWithQuery.ProcessingPhase.Query)
            {           
                this.Partition.Assert(!this.PendingQueries.ContainsKey(clientRequestEvent.EventIdString));

                // Issue a read request that fetches the instance state.
                // We have to buffer this request in the pending list so we can recover it.

                this.PendingQueries.Add(clientRequestEvent.EventIdString, clientRequestEvent);

                if (!effects.IsReplaying)
                {
                    this.Partition.SubmitInternalEvent(new InstanceQueryEvent(clientRequestEvent));
                }
            }
            else 
            {
                this.PendingQueries.Remove(clientRequestEvent.EventIdString);
            }
        }

        public void Process(PurgeBatchIssued purgeBatchIssued, EffectTracker effects)
        {
            var purgeRequest = (PurgeRequestReceived)this.PendingQueries[purgeBatchIssued.QueryEventId];
            purgeRequest.NumberInstancesPurged += purgeBatchIssued.Purged.Count;

            if (!effects.IsReplaying)
            {
                // lets the query that is currently in progress know that this batch is done
                purgeBatchIssued.WhenProcessed.TrySetResult(null);
            }
        }

        internal class InstanceQueryEvent : PartitionQueryEvent
        {
            readonly ClientRequestEventWithQuery request;

            public InstanceQueryEvent(ClientRequestEventWithQuery clientRequest)
            {
                this.request = clientRequest;
            }

            protected override void ExtraTraceInformation(StringBuilder s)
            {
                s.Append(':');
                s.Append(this.request.ToString());
            }

            public override EventId EventId => this.request.EventId;

            public override Netherite.InstanceQuery InstanceQuery => this.request.InstanceQuery;

            public override async Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Partition partition)
            {
                partition.Assert(this.request.Phase == ClientRequestEventWithQuery.ProcessingPhase.Query);

                await this.request.OnQueryCompleteAsync(result, partition);

                var again = (ClientRequestEventWithQuery)this.request.Clone();

                again.NextInputQueuePosition = 0; // this event is no longer considered an external event

                again.Phase = ClientRequestEventWithQuery.ProcessingPhase.Confirm;

                partition.SubmitInternalEvent(again);
            }
        }
    }
}
