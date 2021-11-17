// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
    class InstanceQueryReceived : ClientRequestEventWithQuery
    {
        const int batchsize = 11;

        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> instances, Partition partition)
        {
            int totalcount = 0;

            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationStates = new List<OrchestrationState>()
            };

            using var memoryStream = new MemoryStream();

            await foreach (var orchestrationState in instances)
            {
                if (response.OrchestrationStates.Count == batchsize)
                {
                    response.SerializeOrchestrationStates(memoryStream, this.InstanceQuery.FetchInput);
                    partition.Send(response);
                    response = new QueryResponseReceived
                    {
                        ClientId = this.ClientId,
                        RequestId = this.RequestId,
                        OrchestrationStates = new List<OrchestrationState>()
                    };
                }

                response.OrchestrationStates.Add(orchestrationState);
                totalcount++;
            }

            response.Final = totalcount;
            response.SerializeOrchestrationStates(memoryStream, this.InstanceQuery.FetchInput);
            partition.Send(response);
        }
    }
}
