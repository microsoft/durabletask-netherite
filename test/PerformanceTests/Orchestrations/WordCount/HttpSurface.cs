// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Dynamitey.DynamicObjects;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Extensions.Azure;

    /// <summary>
    /// Defines the REST operations for the word count test.
    /// </summary>
    public static class HttpSurface
    {
        [FunctionName("Wordcount")]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Function, "post", Route = "wordcount")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            var queryParameters = req.Query;
            string action = req.Query["action"];

            if (String.IsNullOrEmpty(action))
            {
                return new BadRequestObjectResult("Please specify an action in the query parameters ?action=");
            }

            switch (action.ToLower())
            {
                case "init":
                    {
                        // initialize mapper and reducer entities prior to running the mapreduce
                        string shape = req.Query["shape"];
                        string[] counts = shape.Split('x');
                        int mapperCount, reducerCount;
                        if (counts.Length == 2 && int.TryParse(counts[0], out mapperCount) && int.TryParse(counts[1], out reducerCount))
                        {
                            var initializationSignals = new List<Task>();
                            // initialize all the mapper entities
                            for (int i = 0; i < mapperCount; i++)
                            {
                                initializationSignals.Add(client.SignalEntityAsync(Mapper.GetEntityId(i), nameof(Mapper.Ops.Init), reducerCount.ToString()));
                            }
                            // initialize all the reducer entities
                            for (int i = 0; i < reducerCount; i++)
                            {
                                initializationSignals.Add(client.SignalEntityAsync(Reducer.GetEntityId(i), nameof(Reducer.Ops.Init), mapperCount.ToString()));
                            }
                            // initialize the summary entity
                            initializationSignals.Add(client.SignalEntityAsync(Summary.GetEntityId(), nameof(Summary.Ops.Init), reducerCount.ToString()));
                            await Task.WhenAll(initializationSignals);
                            return (ActionResult)new OkObjectResult($"Done\n");
                        }
                        else
                        {
                            return new BadRequestObjectResult("Please specify the mapper count and reducer count in the query parameters,  e.g. &shape=10x10");
                        }
                    }

                case "item":
                    {
                        // receive an item and forward it to the specified mapper

                        string mapper = req.Query["mapper"];
                        string data = req.Query["data"];

                        if (String.IsNullOrEmpty(mapper) || !int.TryParse(mapper, out var mapperNumber))
                        {
                            return new BadRequestObjectResult("Please specify the mapper number to send the item to in the query parameters &mapper=");
                        }
                        if (String.IsNullOrEmpty(data))
                        {
                            return new BadRequestObjectResult("Please specify the data to send to the mapper &data=");
                        }
                        await client.SignalEntityAsync(Mapper.GetEntityId(mapperNumber), nameof(Mapper.Ops.Item), data);
                        return (ActionResult)new OkObjectResult($"Done\n");
                    }

                case "end":
                    {
                        // tell the mappers that we have reached the end of the data, and let them tell the reducers
                        string shape = req.Query["shape"];
                        string[] counts = shape.Split('x');
                        int mapperCount, reducerCount;
                        if (counts.Length == 2 && int.TryParse(counts[0], out mapperCount) && int.TryParse(counts[1], out reducerCount))
                        {
                            for (int i = 0; i < mapperCount; i++)
                            {
                                await client.SignalEntityAsync(Mapper.GetEntityId(i), nameof(Mapper.Ops.End));
                            }
                        }
                        else
                        {
                            return new BadRequestObjectResult("Please specify the mapper count and reducer count in the query parameters,  e.g. &shape=10x10");
                        }

                        // wait for summary entity to contain the final result
                        var startWaitingAt = DateTime.UtcNow;
                        int entryCount = 0;
                        List<(int, string)> topWords = null;
                        TimeSpan executionTime = default;
                        do
                        {
                            var summaryState = await client.ReadEntityStateAsync<Summary.SummaryState>(Summary.GetEntityId());
                            if (summaryState.EntityExists && summaryState.EntityState.waitCount == 0)
                            {
                                entryCount = summaryState.EntityState.entryCount;
                                topWords = summaryState.EntityState.topWords;
                                executionTime = summaryState.EntityState.completionTime - summaryState.EntityState.startTime;
                                break;
                            }
                        }
                        while (DateTime.UtcNow < startWaitingAt + TimeSpan.FromMinutes(5));

                        if (topWords == null)
                        {
                            return (ActionResult)new OkObjectResult($"Timed out.\n");
                        }
                        else
                        {
                            var sb = new StringBuilder();
                            sb.AppendLine($"----- {entryCount} words processed in {executionTime.TotalSeconds:F2}s, top 20 as follows -----");
                            foreach ((int count, string word) in topWords)
                            {
                                sb.AppendLine($"{count,10} {word}");
                            }
                            return (ActionResult)new OkObjectResult(sb.ToString());
                        }
                    }

                default:
                    {
                        return new BadRequestObjectResult($"Unknown action: {action}\n");
                    }
            }
        }
    }
}