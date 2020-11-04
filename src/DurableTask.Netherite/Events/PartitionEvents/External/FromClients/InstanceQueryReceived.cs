// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
class InstanceQueryReceived : ClientRequestEventWithQuery
    {
        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> instances, Partition partition)
        {
            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationStates = new List<OrchestrationState>()
            };

            await foreach (var orchestrationState in instances)
            {
                response.OrchestrationStates.Add(orchestrationState);
            }

            partition.Send(response);
        }
    }
}
