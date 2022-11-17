// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
    class InstanceQueryReceived : ClientRequestEventWithQuery
    {
        const int batchsize = 11;

        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> instances, Partition partition, DateTime attempt)
        {
            int totalcount = 0;
            string continuationToken = this.ContinuationToken ?? "";

            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                Attempt = attempt,
                OrchestrationStates = new List<OrchestrationState>(),
            };

            using var memoryStream = new MemoryStream();

            await foreach (var orchestrationState in instances)
            {
                // a null is used to indicate that we have read the last instance
                if (orchestrationState == null)
                {
                    continuationToken = null; // indicates completion
                    break;
                }

                if (response.OrchestrationStates.Count == batchsize)
                {
                    response.SerializeOrchestrationStates(memoryStream, this.InstanceQuery.FetchInput);
                    partition.Send(response);
                    response = new QueryResponseReceived
                    {
                        ClientId = this.ClientId,
                        RequestId = this.RequestId,
                        Attempt = attempt,
                        OrchestrationStates = new List<OrchestrationState>(),
                    };
                }

                response.OrchestrationStates.Add(orchestrationState);
                continuationToken = orchestrationState.OrchestrationInstance.InstanceId;
                totalcount++;
            }

            response.Final = totalcount;
            response.ContinuationToken = continuationToken;
            response.SerializeOrchestrationStates(memoryStream, this.InstanceQuery.FetchInput);
            partition.Send(response);

            partition.EventTraceHelper.TraceEventProcessingDetail($"query {this.EventId} attempt {attempt:o} responded totalcount={totalcount} continuationToken={response.ContinuationToken ?? "null"}");
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }
    }
}
