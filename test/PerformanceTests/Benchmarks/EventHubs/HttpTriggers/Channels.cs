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
    using System.Net;

    public static class Channels
    {
        [FunctionName("EH_" + nameof(Channels))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/channels/{action}")] HttpRequest req,
           [DurableClient] IDurableClient client,
           string action,
           ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numEventHubs = int.Parse(requestBody);

                if (numEventHubs < 1 || numEventHubs > Parameters.MaxEventHubs)
                {
                    return new ObjectResult("invalid number of event hubs.\n") { StatusCode = (int)HttpStatusCode.BadRequest };
                }

                switch (action)
                {             
                    case "create":

                        log.LogWarning($"Creating {numEventHubs} EventHubs...");
                        int numCreated = 0;
                        await Enumerable.Range(0, numEventHubs).ParallelForEachAsync(200, false, async (index) =>
                        {
                            if (await EventHubsUtil.EnsureEventHubExistsAsync(Parameters.EventHubConnectionString, Parameters.EventHubName(index), Parameters.MaxPartitionsPerEventHub))
                            {
                                numCreated++;
                            }
                        });
                        return new OkObjectResult($"Created {numCreated} EventHubs.\n");

                    case "delete":

                        log.LogWarning($"Deleting {numEventHubs} EventHubs...");
                        int numDeleted = 0;
                        await Enumerable.Range(0, numEventHubs).ParallelForEachAsync(200, false, async (index) =>
                        {
                            if (await EventHubsUtil.DeleteEventHubIfExistsAsync(Parameters.EventHubConnectionString, Parameters.EventHubName(index)))
                            {
                                numDeleted++;
                            }
                        });

                        return new OkObjectResult($"Deleted {numDeleted} EventHubs.\n");

                    default:
                        return new ObjectResult($"Unknown action: {action}\n")
                        {
                            StatusCode = (int)System.Net.HttpStatusCode.NotFound
                        };
                }
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() }) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
