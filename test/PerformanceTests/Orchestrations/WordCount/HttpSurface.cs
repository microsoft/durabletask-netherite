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
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Defines the REST operations for the word count test.
    /// </summary>
    public static class HttpSurface
    {
        [FunctionName("Wordcount")]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "wordcount")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            var queryParameters = req.Query;

            // get mapper and reducer count from shape parameter
            string shape = req.Query["shape"];
            string[] counts = shape.Split('x');
            int mapperCount, reducerCount;

            // get list of URLs from request body
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            string[] urls = requestBody.Split();

            if (!(counts.Length == 2 && int.TryParse(counts[0], out mapperCount) && int.TryParse(counts[1], out reducerCount)))
            {
                return new BadRequestObjectResult("Please specify the mapper count and reducer count in the query parameters,  e.g. &shape=10x10");
            }

            // ----- PHASE 1 ----------
            // initialize all three types of entities prior to running the mapreduce

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
            
            // we want to have the initialization completed before we start the test
            await Task.WhenAll(initializationSignals);

            // ----- PHASE 2 ----------
            // send work to the mappers

            int urlCount = 0;

            foreach (var url in urls)
            {
                var trimmedUrl = url.Trim();
                if (Uri.IsWellFormedUriString(trimmedUrl, UriKind.Absolute))
                {
                    int mapper = urlCount++ % mapperCount;
                    var _ = client.SignalEntityAsync(Mapper.GetEntityId(mapper), nameof(Mapper.Ops.Item), url);
                }
            }

            for (int i = 0; i < mapperCount; i++)
            {
                var _ = client.SignalEntityAsync(Mapper.GetEntityId(i), nameof(Mapper.Ops.End));
            }

            // ----- PHASE 3 ----------
            // wait for summary entity to contain the final result

            var startWaitingAt = DateTime.UtcNow;
            int entryCount = 0;
            List<(int, string)> topWords = null;
            double executionTime = 0;
            do
            {
                await Task.Delay(500);
                var summaryState = await client.ReadEntityStateAsync<Summary.SummaryState>(Summary.GetEntityId());
                if (summaryState.EntityExists && summaryState.EntityState.waitCount == 0)
                {
                    entryCount = summaryState.EntityState.entryCount;
                    topWords = summaryState.EntityState.topWords;
                    executionTime = summaryState.EntityState.executionTimeInSeconds;
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
                sb.AppendLine($"----- {urlCount} urls with {entryCount} words processed in {executionTime:F2}s, top 20 as follows -----");
                foreach ((int count, string word) in topWords)
                {
                    sb.AppendLine($"{count,10} {word}");
                }
                return (ActionResult)new OkObjectResult(sb.ToString());
            }
        }
    }
}