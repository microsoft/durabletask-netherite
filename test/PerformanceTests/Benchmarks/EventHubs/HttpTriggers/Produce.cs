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

    public static class Produce
    {
        // Populates the event hubs with events. This should be called prior to consuming the events.
        //
        // for example, to distribute 1000 events of 1k each over 12 partitions, call
        //      curl http://localhost:7071/eh/produce -d 1000x1024/12

        [FunctionName("EH_" + nameof(Produce))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/" + nameof(Produce))] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int xpos = requestBody.IndexOf('x');
                int spos = requestBody.IndexOf('/');
                int numEvents = int.Parse(requestBody.Substring(0, xpos));
                int payloadSize = int.Parse(requestBody.Substring(xpos + 1, spos - (xpos + 1)));
                int numPartitions = int.Parse(requestBody.Substring(spos + 1));
                int producerBatchSize = 500 * 1024 / payloadSize;
                string volume = $"{1.0 * payloadSize * numEvents / (1024 * 1024):F2}MB";

                string connectionString = Environment.GetEnvironmentVariable(Parameters.EventHubsConnectionName);
                
                await EventHubsUtil.EnsureEventHubExistsAsync(connectionString, Parameters.EventHubName, numPartitions);
                await EventHubsUtil.EnsureEventHubExistsAsync(connectionString, "tasks", 32);

                var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                {
                    EntityPath = Parameters.EventHubName,
                };
                var ehc = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                var ehInfo = await ehc.GetRuntimeInformationAsync();

                if (ehInfo.PartitionCount != numPartitions)
                {
                    throw new ArgumentException("Wrong number of partitions");
                }

                async Task SendEventsAsync(int offset, int events, string partitionId)
                {
                    var partitionSender = ehc.CreatePartitionSender(partitionId);
                    var eventBatch = new List<EventData>(100);
                    for (int x = 0; x < events; x++)
                    {
                        byte[] payload = new byte[payloadSize];
                        (new Random()).NextBytes(payload);
                        payload[0] = byte.Parse(partitionId);
                        var e = new EventData(payload);
                        eventBatch.Add(e);

                        if (x % producerBatchSize == (producerBatchSize - 1))
                        {
                            await partitionSender.SendAsync(eventBatch);
                            log.LogWarning($"Sent {x + 1} events to partition {partitionId}");
                            eventBatch = new List<EventData>(100);
                        }
                    }
                    if (eventBatch.Count > 0)
                    {
                        await partitionSender.SendAsync(eventBatch);
                        log.LogWarning($"Sent {events} events to partition {partitionId}");
                    }
                }

                log.LogWarning($"Sending {numEvents} EventHub events ({volume}) to {numPartitions} partitions");
                var partitionTasks = new List<Task>();
                var remaining = numEvents;
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = numPartitions; i >= 1; i--)
                {
                    var portion = remaining / i;
                    remaining -= portion;
                    partitionTasks.Add(SendEventsAsync(remaining, portion, ehInfo.PartitionIds[i - 1]));
                }

                await Task.WhenAll(partitionTasks);

                double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

                log.LogWarning($"Sent all {numEvents} EventHub events ({volume}) to {numPartitions} partitions in {elapsedSeconds}");

                var resultObject = new { numEvents, volume, numPartitions, elapsedSeconds };
                return new OkObjectResult($"{JsonConvert.SerializeObject(resultObject)}\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }
    }
}
