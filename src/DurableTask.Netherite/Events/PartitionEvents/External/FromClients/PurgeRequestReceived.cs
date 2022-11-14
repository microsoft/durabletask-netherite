// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
    class PurgeRequestReceived : ClientRequestEventWithQuery
    {
        // tracks total number of purges that happened
        // it is stored in QueriesState so it correctly counts all successful purges 
        // even if a query is restarted before completing
        [DataMember]
        public int NumberInstancesPurged { get; set; } = 0;

        const int MaxBatchSize = 1000;

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> instances, Partition partition, DateTime attempt)
        {
            int batchCount = 0;
            string continuationToken = null;

            PurgeBatchIssued makeNewBatchObject()
                => new PurgeBatchIssued()
                {
                    PartitionId = partition.PartitionId,
                    QueryEventId = this.EventIdString,
                    BatchNumber = batchCount++,
                    InstanceIds = new List<string>(),
                    WhenProcessed = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously),
                    InstanceQuery = this.InstanceQuery,
                };

            PurgeBatchIssued batch = makeNewBatchObject();

            // TODO : while the request itself is reliable, the client response is not. 
            // We should probably fix that by using the ClientState to track progress.

            async Task ExecuteBatch()
            {
                await partition.State.Prefetch(batch.KeysToPrefetch);
                partition.SubmitEvent(batch);
                await batch.WhenProcessed.Task;
            }

            await foreach (var orchestrationState in instances)
            {
                if (orchestrationState != null)
                {
                    batch.InstanceIds.Add(orchestrationState.OrchestrationInstance.InstanceId);

                    if (batch.InstanceIds.Count == MaxBatchSize)
                    {
                        await ExecuteBatch();
                        batch = makeNewBatchObject();
                    }
                }
            }

            if (batch.InstanceIds.Count > 0)
            {
                await ExecuteBatch();
            }

            partition.Send(new PurgeResponseReceived()
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                NumberInstancesPurged = this.NumberInstancesPurged,
                ContinuationToken = continuationToken,
            });
        }
    }
}
