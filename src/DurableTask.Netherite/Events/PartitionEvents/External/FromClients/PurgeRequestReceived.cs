// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
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

        public async override Task OnQueryCompleteAsync(
            IAsyncEnumerable<OrchestrationState> instances, 
            Partition partition,
            PartitionQueryEvent evt)
        {
            int batchCount = 0;

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

            await foreach (var orchestrationState in instances)
            {
                batch.InstanceIds.Add(orchestrationState.OrchestrationInstance.InstanceId);

                if (batch.InstanceIds.Count == MaxBatchSize)
                {
                    await ExecuteBatch();
                    batch = makeNewBatchObject();
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
            });
        }
    }
}
