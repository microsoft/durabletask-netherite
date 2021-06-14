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
    using Microsoft.Extensions.Logging;
    using System.Linq;
    using Newtonsoft.Json;

    public static class Count
    {
        [FunctionName("EH_" + nameof(Count))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/" + nameof(Count))] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numEntities = int.Parse(requestBody);

                int numEvents = 0;
                long earliestStart = long.MaxValue;
                long latestUpdate = 0;
                long bytesReceived = 0;
                bool gotTimeRange = false;

                object lockForUpdate = new object();

                var tasks = new List<Task<bool>>();

                log.LogWarning($"Checking the status of {numEntities} entities...");
                await Enumerable.Range(0, numEntities).ParallelForEachAsync(200, true, async (index) =>
                {
                    var entityId = ReceiverEntity.GetEntityId(index);
                    var response = await client.ReadEntityStateAsync<ReceiverEntity>(entityId);
                    if (response.EntityExists)
                    {
                        lock (lockForUpdate)
                        {
                            numEvents += response.EntityState.EventCount;
                            bytesReceived += response.EntityState.BytesReceived;
                            earliestStart = Math.Min(earliestStart, response.EntityState.StartTime.Ticks);
                            latestUpdate = Math.Max(latestUpdate, response.EntityState.LastTime.Ticks);
                            gotTimeRange = true;
                        }
                    }
                });

                double elapsedSeconds = 0;

                if (gotTimeRange)
                {
                    elapsedSeconds = (new DateTime(latestUpdate) - new DateTime(earliestStart)).TotalSeconds;
                }

                string volume = $"{1.0 * bytesReceived / (1024 * 1024):F2}MB";

                log.LogWarning($"Received a total of {numEvents} ({volume}) on {numEntities} entities in {elapsedSeconds:F2}s.");

                var resultObject = new
                {
                    numEvents,
                    bytesReceived,
                    volume,
                    elapsedSeconds,
                };

                return new OkObjectResult($"{JsonConvert.SerializeObject(resultObject)}\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }
    }
}
