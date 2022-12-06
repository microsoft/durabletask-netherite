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

        const int MaxBatchSize = 200;

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<(string,OrchestrationState)> instances, Partition partition, DateTime attempt)
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

            async Task ExecuteBatch()
            {
                await partition.State.Prefetch(batch.KeysToPrefetch);
                partition.SubmitEvent(batch);
                await batch.WhenProcessed.Task;
            }

            await foreach (var (position, instance) in instances)
            {
                if (instance != null)
                {
                    string instanceId = instance.OrchestrationInstance.InstanceId;

                    batch.InstanceIds.Add(instanceId);

                    if (batch.InstanceIds.Count == MaxBatchSize)
                    {
                        await ExecuteBatch();
                        batch = makeNewBatchObject();
                    }

                    continuationToken = position;
                }
                else
                {
                    continuationToken = position;
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
