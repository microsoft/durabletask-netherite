// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

        public override void Process(RecoveryCompleted evt, EffectTracker effects)
        {
            var timedOut = this.PendingQueries.Where(kvp => kvp.Value.TimeoutUtc < evt.Timestamp).ToList();

            foreach (var kvp in timedOut)
            {
                this.PendingQueries.Remove(kvp.Key);

                if (!effects.IsReplaying)
                {
                    effects.EventTraceHelper.TraceEventProcessingWarning($"Dropped query {kvp.Value.EventIdString} because it has timed out");
                }
            }

            if (!effects.IsReplaying)
            {
                // reissue queries that did not complete prior to crash/recovery
                foreach (var kvp in this.PendingQueries)
                {
                    this.Partition.SubmitParallelEvent(new InstanceQueryEvent(kvp.Value));
                }
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

        public override void Process(InstanceQueryReceived clientRequestEvent, EffectTracker effects)
        {
            this.ProcessClientRequestEventWithQuery(clientRequestEvent, effects);
        }

        public override void Process(PurgeRequestReceived clientRequestEvent, EffectTracker effects)
        {
            this.ProcessClientRequestEventWithQuery(clientRequestEvent, effects);
        }

        void ProcessClientRequestEventWithQuery(ClientRequestEventWithQuery clientRequestEvent, EffectTracker effects)
        {
            if (clientRequestEvent.Phase == ClientRequestEventWithQuery.ProcessingPhase.Query)
            {           
                this.Partition.Assert(!this.PendingQueries.ContainsKey(clientRequestEvent.EventIdString), "key already there in QueriesState");
                // Buffer this request in the pending list so we can recover it.
                this.PendingQueries.Add(clientRequestEvent.EventIdString, clientRequestEvent);
            }
            else 
            {
                this.Partition.Assert(clientRequestEvent.Phase == ClientRequestEventWithQuery.ProcessingPhase.Confirm, "wrong phase in QueriesState");
                this.PendingQueries.Remove(clientRequestEvent.EventIdString);
            }
        }

        public override void Process(PurgeBatchIssued purgeBatchIssued, EffectTracker effects)
        {
            var purgeRequest = (PurgeRequestReceived)this.PendingQueries[purgeBatchIssued.QueryEventId];
            purgeRequest.NumberInstancesPurged += purgeBatchIssued.Purged.Count;

            if (!effects.IsReplaying)
            {
                // lets the query that is currently in progress know that this batch is done
                DurabilityListeners.Register(purgeBatchIssued, purgeBatchIssued);
            }
        }

        /// <summary>
        /// This event represents the execution of the actual query. It is a read-only
        /// query event.
        /// </summary>
        internal class InstanceQueryEvent : PartitionQueryEvent
        {
            readonly ClientRequestEventWithQuery request;

            public override DateTime? TimeoutUtc => this.request.TimeoutUtc;

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
                partition.Assert(this.request.Phase == ClientRequestEventWithQuery.ProcessingPhase.Query, "wrong phase in QueriesState.OnQueryCompleteAsync");

                try
                {
                    await this.request.OnQueryCompleteAsync(result, partition);
                }
                catch (TimeoutException)  // we catch them so we can mark the query as completed
                {
                    partition.EventTraceHelper.TraceEventProcessingWarning($"query {this.request.EventId} timed out");
                }

                // we now how to recycle the request event again in order to remove it from the list of pending queries
                var again = (ClientRequestEventWithQuery)this.request.Clone();
                again.NextInputQueuePosition = 0; // this event is no longer considered an external event        
                again.Phase = ClientRequestEventWithQuery.ProcessingPhase.Confirm;
                partition.SubmitEvent(again);
            }
        }
    }
}
