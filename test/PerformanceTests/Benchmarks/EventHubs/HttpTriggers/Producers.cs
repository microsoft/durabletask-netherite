// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventProducer
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using EventHubs;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.AspNetCore.Routing;
    using Microsoft.Azure.WebJobs;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Newtonsoft.Json;
    using System.Linq;
    using System.Diagnostics;
    using System.IO;
    using System.Net;

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

                if (numProducers < 1 || numProducers > Parameters.MaxProducers)
                {
                    return new ObjectResult("invalid number of producers.\n") { StatusCode = (int)HttpStatusCode.BadRequest };
                }

                switch (action)
                {
                    case "query":

                        object lockForUpdate = new object();

                        long sent = 0;
                        long exceptions = 0;
                        int active = 0;
                       
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
                                }
                            }
                        });

                        var resultObject = new
                        {
                            sent,
                            exceptions,
                            active,
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
