// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventProducer
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using EventHubs;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public static class Producers
    {
        [FunctionName("EH_" + nameof(Producers))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/producers/{action}")] HttpRequest req,
           [DurableClient] IDurableClient client,
           string action,
           ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numProducers = int.Parse(requestBody);

                switch (action)
                {
                    case "query":

                        object lockForUpdate = new object();

                        long sent = 0;
                        long exceptions = 0;
                        int active = 0;
                        DateTime? Starttime = null;
                        DateTime? LastUpdate = null;

                        log.LogWarning("Checking the status of {NumProducers} producer entities...", numProducers);
                        await Enumerable.Range(0, numProducers).ParallelForEachAsync(500, true, async (partition) =>
                        {
                            var entityId = ProducerEntity.GetEntityId(partition);
                            var response = await client.ReadEntityStateAsync<ProducerEntity>(entityId);
                            if (response.EntityExists)
                            {
                                lock (lockForUpdate)
                                {
                                    sent += response.EntityState.SentEvents;
                                    exceptions += response.EntityState.Exceptions;
                                    active += response.EntityState.IsActive ? 1 : 0;

                                    if (!Starttime.HasValue || response.EntityState.Starttime < Starttime)
                                    {
                                        Starttime = response.EntityState.Starttime;
                                    }
                                    if (!LastUpdate.HasValue || response.EntityState.LastUpdate > LastUpdate)
                                    {
                                        LastUpdate = response.EntityState.LastUpdate;
                                    }
                                }
                            }
                        });

                        double? throughput = null;
                        if (Starttime.HasValue && LastUpdate.HasValue)
                        {
                            throughput = 1.0 * sent / (LastUpdate.Value - Starttime.Value).TotalSeconds;
                        }

                        var resultObject = new
                        {
                            sent,
                            exceptions,
                            active,
                            throughput,
                        };

                        return new OkObjectResult($"{JsonConvert.SerializeObject(resultObject)}\n");

                    case "start":
                    case "stop":
                    case "delete":

                        await Enumerable.Range(0, numProducers).ParallelForEachAsync(500, true, async (number) =>
                        {
                            var entityId = ProducerEntity.GetEntityId(number);
                            await client.SignalEntityAsync(entityId, action);
                        });

                        return new OkObjectResult($"sent {action} signal to {numProducers} producer entities\n");

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
