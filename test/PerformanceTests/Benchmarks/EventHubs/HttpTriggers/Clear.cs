// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Producer;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Logging;

    public static class Clear
    {
        [FunctionName("EH_" + nameof(Clear))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/" + nameof(Clear))] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            try 
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numEntities = int.Parse(requestBody);

                log.LogWarning($"Deleting {numEntities + 32} entities...");
                await Enumerable.Range(0, numEntities + 32).ParallelForEachAsync(200, true, async (index) =>
                {
                    if (index <= numEntities)
                    {
                        var entityId = ReceiverEntity.GetEntityId(index);
                        await client.SignalEntityAsync(entityId, "delete");
                    }
                    else
                    {
                        int partition = index - numEntities;
                        await client.SignalEntityAsync(PartitionReceiverEntity.GetEntityId(Parameters.EventHubName, partition.ToString()), "delete");
                    }
                });
                
                return new OkObjectResult($"Deleted {numEntities + 32} entities.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(new { 
                    error = e.ToString()
                });
            }
        }
    }
}
