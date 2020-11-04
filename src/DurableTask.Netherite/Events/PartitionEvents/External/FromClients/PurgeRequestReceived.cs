// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> instances, Partition partition)
        {
            int batchCount = 0;

            PurgeBatchIssued makeBatchObject()
                => new PurgeBatchIssued()
                {
                    PartitionId = partition.PartitionId,
                    QueryEventId = this.EventIdString,
                    BatchNumber = batchCount++,
                    InstanceIds = new List<string>(),
                    WhenProcessed = new TaskCompletionSource<object>(),
                    InstanceQuery = this.InstanceQuery,
                };

            PurgeBatchIssued batch = makeBatchObject();

            await foreach (var orchestrationState in instances)
            {
                batch.InstanceIds.Add(orchestrationState.OrchestrationInstance.InstanceId);

                if (batch.InstanceIds.Count == MaxBatchSize)
                {
                    partition.SubmitInternalEvent(batch);
                    await batch.WhenProcessed.Task;
                    makeBatchObject();
                }
            }

            if (batch.InstanceIds.Count > 0)
            {
                partition.SubmitInternalEvent(batch);
                await batch.WhenProcessed.Task;
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
