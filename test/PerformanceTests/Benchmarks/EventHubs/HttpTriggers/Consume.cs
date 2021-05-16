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

    public static class Consume
    {
        [FunctionName("EH_" + nameof(Consume))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "eh/" + nameof(Consume))] HttpRequest req,
           [DurableClient] IDurableClient client)
        {
            try 
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numEntities = int.Parse(requestBody);

                var connectionString = Environment.GetEnvironmentVariable("EventHubsConnection");
                var eventHubName = Parameters.EventHubName;
                var producer = new EventHubProducerClient(connectionString, eventHubName);
                var partitionIds = await producer.GetPartitionIdsAsync();

                List<Task> signalTasks = new List<Task>();
                foreach (var partitionId in partitionIds)
                {
                    var partitionProperties = await producer.GetPartitionPropertiesAsync(partitionId);
                    var startParameters = new PartitionReceiverEntity.StartParameters()
                    {
                        NumEntities = numEntities,
                        PartitionId = partitionId,
                        NextSequenceNumberToReceive = 0,
                    };
                    signalTasks.Add(client.SignalEntityAsync(PartitionReceiverEntity.GetEntityId(eventHubName, partitionId), nameof(PartitionReceiverEntity.Start), startParameters));
                }
                await Task.WhenAll(signalTasks);

                return new OkObjectResult($"Started PartitionReceiverEntities for {signalTasks.Count} partitions.\n");
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
