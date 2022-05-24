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
    using System.Net;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public static class Consumers
    {
        [FunctionName("EH_" + nameof(Consumers))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/consumers/{action}")] HttpRequest req,
           [DurableClient] IDurableClient client,
           string action,
           ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numPullers = int.Parse(requestBody);

                if (numPullers < 1 || numPullers > Parameters.MaxPartitions)
                {
                    return new ObjectResult("invalid number of consumers.\n") { StatusCode = (int)HttpStatusCode.BadRequest };
                }

                switch (action)
                {
                    case "query":

                        object lockForUpdate = new object();

                        int delivered = 0;
                        long pulled = 0;
                        int pending = 0;
                        int active = 0;
                        DateTime? firstReceived = null;
                        DateTime? lastUpdated = null;
                        int errors = 0;

                        log.LogWarning($"Checking the status of {numPullers} puller entities...");                    
                        await Enumerable.Range(0, numPullers).ParallelForEachAsync(500, true, async (partition) =>
                        {
                            var entityId = PullerEntity.GetEntityId(partition);
                            var response = await client.ReadEntityStateAsync<PullerEntity>(entityId);
                            if (response.EntityExists)
                            {
                                lock (lockForUpdate)
                                {
                                    pulled += response.EntityState.TotalEventsPulled;
                                    pending += response.EntityState.NumPending;
                                    active += response.EntityState.IsActive ? 1 : 0;
                                    errors += response.EntityState.Errors;

                                    if (!firstReceived.HasValue || response.EntityState.FirstReceived < firstReceived)
                                    {
                                        firstReceived = response.EntityState.FirstReceived;
                                    }
                                }
                            }
                        });

                        log.LogWarning($"Checking the status of {Parameters.NumberDestinationEntities} destination entities...");                    
                        await Enumerable.Range(0, Parameters.NumberDestinationEntities).ParallelForEachAsync(500, true, async (destination) =>
                        {
                            var entityId = DestinationEntity.GetEntityId(destination);
                            var response = await client.ReadEntityStateAsync<DestinationEntity>(entityId);
                            if (response.EntityExists)
                            {
                                lock (lockForUpdate)
                                {
                                    delivered += response.EntityState.EventCount;

                                    if (!lastUpdated.HasValue || response.EntityState.LastUpdated > lastUpdated)
                                    {
                                        lastUpdated = response.EntityState.LastUpdated;
                                    }
                                }
                            }
                        });

                        TimeSpan? duration = lastUpdated - firstReceived;

                        var resultObject = new
                        {
                            pending,
                            active,
                            delivered,
                            pulled,
                            errors,
                            firstReceived,
                            lastUpdated,
                            duration
                        };

                        return new OkObjectResult($"{JsonConvert.SerializeObject(resultObject)}\n");

                    case "start":
                    case "stop":

                        await Enumerable.Range(0, numPullers).ParallelForEachAsync(200, true, async (number) =>
                        {
                            var entityId = PullerEntity.GetEntityId(number);
                            await client.SignalEntityAsync(entityId, action);
                        });

                        return new OkObjectResult($"Sent {action} signal to {numPullers} puller entities.\n");


                    case "delete":

                        await Enumerable.Range(0, numPullers + Parameters.NumberDestinationEntities).ParallelForEachAsync(500, true, async (i) =>
                        {
                            var entityId = i < numPullers ? PullerEntity.GetEntityId(i) : DestinationEntity.GetEntityId(i - numPullers);
                            await client.SignalEntityAsync(entityId, action);
                        });

                        return new OkObjectResult($"Sent delete signal to {numPullers} puller entities and {Parameters.NumberDestinationEntities} destination entities.\n");

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
