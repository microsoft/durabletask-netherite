// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.CollisionSearch
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    public static class HttpTriggers
    {
        /// <summary>
        /// Http trigger to start a collision search and wait for the result.
        /// 
        /// </summary>
        [FunctionName(nameof(CollisionSearch))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "CollisionSearch")] HttpRequest req,
           [DurableClient] IDurableClient client)
        {
            try
            {
                // get the number of tasks
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                double billions = double.Parse(requestBody);

                var parameters = new CollisionSearch.IntervalSearchParameters()
                {
                    Target = 100007394059441.GetHashCode(), // we use a value that is guaranteed to have at least one "collision"
                    Start = 100000000000000,
                    Count = (long)(billions * 1000000000),
                };

                // start the orchestration
                string orchestrationInstanceId = await client.StartNewAsync(nameof(CollisionSearch.SearchOrchestration), null, parameters);

                // wait for it to complete and return the result
                return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }
    }
}
