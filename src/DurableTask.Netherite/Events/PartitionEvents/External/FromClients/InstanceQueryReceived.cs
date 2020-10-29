//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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
