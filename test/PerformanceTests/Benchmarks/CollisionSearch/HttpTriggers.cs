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
    using System.Net.Http;
    using Newtonsoft.Json.Linq;
    using System.Net;

    public static class HttpTriggers
    {
        /// <summary>
        /// Http trigger to start a collision search and wait for the result.
        /// 
        /// </summary>
        [FunctionName(nameof(CollisionSearch))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "CollisionSearch/{algorithm}")] HttpRequest req,
           string algorithm,
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
                string orchestrationName;

                switch (algorithm)
                {
                    case "divide-and-conquer":
                        orchestrationName = nameof(CollisionSearch.DivideAndConquerSearch);
                        break;

                    case "flat-parallel":
                        orchestrationName = nameof(CollisionSearch.FlatParallelSearch);
                        break;

                    default:
                        throw new ArgumentException("unknown algorithm");
                }

                string orchestrationInstanceId = await client.StartNewAsync(orchestrationName, null, parameters);

                // wait for it to complete and return the result
                var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(900));

                if (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                    && responseMessage.Content is StringContent stringContent)
                {
                    var state = await client.GetStatusAsync(orchestrationInstanceId, false, false, false);
                    var result = (JArray) JToken.Parse(await stringContent.ReadAsStringAsync());
                    var collisionsFound = result.Count;
                    var elapsedSeconds = (state.LastUpdatedTime - state.CreatedTime).TotalSeconds;
                    var size = billions;
                    var throughput = size / elapsedSeconds;
                    response = new OkObjectResult(new { collisionsFound, elapsedSeconds, size, throughput });                
                }

                return response;
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() }) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
