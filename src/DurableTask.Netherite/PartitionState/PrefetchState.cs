﻿// Copyright (c) Microsoft Corporation.
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
    class PrefetchState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, ClientRequestEventWithPrefetch> PendingPrefetches { get; private set; } = new Dictionary<string, ClientRequestEventWithPrefetch>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Prefetch);

        public override void Process(RecoveryCompleted evt, EffectTracker effects)
        {
            var timedOut = this.PendingPrefetches.Where(kvp => kvp.Value.TimeoutUtc < evt.Timestamp).ToList();

            foreach (var kvp in timedOut)
            {
                this.PendingPrefetches.Remove(kvp.Key);

                if (!effects.IsReplaying)
                {
                    effects.EventTraceHelper.TraceEventProcessingWarning($"Dropped request {kvp.Value.EventIdString} during recovery, because it has timed out");
                }
            }

            if (!effects.IsReplaying)
            {
                // reissue prefetch tasks for what did not complete prior to crash/recovery
                foreach (var kvp in this.PendingPrefetches)
                {
                    this.Partition.SubmitParallelEvent(new InstancePrefetch(kvp.Value));
                }
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Requests += this.PendingPrefetches.Count;
        }

        public override string ToString()
        {
            return $"Prefetch ({this.PendingPrefetches.Count} pending)";
        }

        public override void Process(CreationRequestReceived creationRequestEvent, EffectTracker effects)
        {
            this.ProcessClientRequestEventWithPrefetch(creationRequestEvent, effects);
        }

        public override void Process(DeletionRequestReceived deletionRequestEvent, EffectTracker effects)
        {
            this.ProcessClientRequestEventWithPrefetch(deletionRequestEvent, effects);
        }

        public override void Process(WaitRequestReceived waitRequestEvent, EffectTracker effects)
        {
            this.ProcessClientRequestEventWithPrefetch(waitRequestEvent, effects);
        }

        void ProcessClientRequestEventWithPrefetch(ClientRequestEventWithPrefetch clientRequestEvent, EffectTracker effects)
        {
            if (clientRequestEvent.Phase == ClientRequestEventWithPrefetch.ProcessingPhase.Read)
            {
                // It's possible for EventHubs to duplicate client-to-partition events. Therefore, we perform a best-effort
                // de-duplication of EH messages. For more details, see: https://github.com/microsoft/durabletask-netherite/pull/379
                if (!this.PendingPrefetches.ContainsKey(clientRequestEvent.EventIdString))
                {
                    // Issue a read request that fetches the instance state.
                    // We buffer this request in the pending list so we can recover it, and can filter duplicates
                    // (as long as the duplicates appear soon after the original)

                    this.PendingPrefetches.Add(clientRequestEvent.EventIdString, clientRequestEvent);
                }
                else
                {
                    // this is a duplicate. Ignore it.
                    effects.EventTraceHelper?.TraceEventProcessingWarning($"Dropped duplicate client request {clientRequestEvent} id={clientRequestEvent.EventIdString}");
                    return; 
                }
            }
            else 
            {
                if (this.PendingPrefetches.Remove(clientRequestEvent.EventIdString))
                {
                    if (clientRequestEvent.Phase == ClientRequestEventWithPrefetch.ProcessingPhase.ConfirmAndProcess)
                    {
                        effects.Add(clientRequestEvent.Target);
                    }
                }
            }
        }

        internal class InstancePrefetch : InternalReadEvent
        {
            readonly ClientRequestEventWithPrefetch request;

            public InstancePrefetch(ClientRequestEventWithPrefetch clientRequest)
            {
                this.request = clientRequest;
            }

            protected override void ExtraTraceInformation(StringBuilder s)
            {
                s.Append(':');
                s.Append(this.request.ToString());
                base.ExtraTraceInformation(s);
            }

            public override TrackedObjectKey ReadTarget => this.request.Target;

            public override TrackedObjectKey? Prefetch => this.request.Prefetch;

            public override EventId EventId => this.request.EventId;

            public override void OnReadComplete(TrackedObject target, Partition partition)
            {
                partition.Assert(this.request.Phase == ClientRequestEventWithPrefetch.ProcessingPhase.Read, "wrong phase in PrefetchState");

                bool requiresProcessing = this.request.OnReadComplete(target, partition);

                var again = (ClientRequestEventWithPrefetch) this.request.Clone();

                again.NextInputQueuePosition = 0; // this event is no longer considered an external event

                again.Phase = requiresProcessing ?
                    ClientRequestEventWithPrefetch.ProcessingPhase.ConfirmAndProcess : ClientRequestEventWithPrefetch.ProcessingPhase.Confirm;
                 
                partition.SubmitEvent(again);
            }
        }
    }
}
