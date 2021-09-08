// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
    class InstanceQueryReceived : ClientRequestEventWithQuery
    {
        public async override Task OnQueryCompleteAsync(
            IAsyncEnumerable<OrchestrationState> instances, 
            Partition partition,
            PartitionQueryEvent evt)
        {
            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationStates = new List<OrchestrationState>(),
            };

            await foreach (var orchestrationState in instances)
            {
                response.OrchestrationStates.Add(orchestrationState.ClearFieldsImmutably(!evt.InstanceQuery.FetchInput, false));
            }

            partition.Assert(response.OrchestrationStates.Count == evt.PageSizeResult);
            response.ContinuationToken = evt.ContinuationTokenResult;
            partition.Send(response);
        }
    }
}
