﻿// Copyright (c) Microsoft Corporation.
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
    using System.Collections.Generic;
    using Newtonsoft.Json;
    using System.Threading;
    using Microsoft.Extensions.Logging;

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

                    case "flat-parallel-http":
                        orchestrationName = nameof(CollisionSearch.FlatParallelHttpSearch);
                        break;

                    default:
                        throw new ArgumentException("unknown algorithm");
                }

                string orchestrationInstanceId = await client.StartNewAsync(orchestrationName, null, parameters);

                // wait for it to complete and return the result
                var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(60));

                if (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                    && responseMessage.Content is StringContent stringContent)
                {
                    var state = await client.GetStatusAsync(orchestrationInstanceId, false, false, false);
                    var stringresult = await stringContent.ReadAsStringAsync();
                    try
                    {
                        List<long> result = JsonConvert.DeserializeObject<List<long>>(stringresult);
                        var collisionsFound = result.Count;
                        var elapsedSeconds = (state.LastUpdatedTime - state.CreatedTime).TotalSeconds;
                        var size = billions;
                        var throughput = size / elapsedSeconds;
                        response = new OkObjectResult(new { collisionsFound, elapsedSeconds, size, throughput });
                    }
                    catch (Exception e)
                    {
                        return new ObjectResult(new { error = e.ToString(), stringresult });
                    }
                }

                return response;
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }

        /// <summary>
        /// Http trigger to search a portion.
        /// 
        /// </summary>
        [FunctionName(nameof(CollisionSearchPortion))]
        public static async Task<IActionResult> CollisionSearchPortion(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "CollisionSearch/Portion/{target}/{start}/{count}")] HttpRequest req,
           int target,
           long start,
           long count,
           ILogger logger)
        {
            ObjectResult result;

            if (await LocalLimit.WaitAsync(TimeSpan.FromSeconds(100 + 10 * (new Random()).NextDouble())))
            {
                //logger.LogWarning($"Start conc={LocalLimit.CurrentCount}");
                try
                {

                    var results = new List<long>();
                    for (var i = start; i < start + count; i++)
                    {
                        if (i.GetHashCode() == target)
                            results.Add(i);
                    }

                    result = new OkObjectResult(JsonConvert.SerializeObject(results));
                }
                catch (Exception e)
                {
                    result = new ObjectResult(new { error = e.ToString() });
                    result.StatusCode = (int) System.Net.HttpStatusCode.InternalServerError;
                }
                finally
                {
                    //logger.LogWarning($"Complete conc={LocalLimit.CurrentCount}");
                    LocalLimit.Release();
                }
            }
            else
            {
                //logger.LogWarning($"Reject conc={LocalLimit.CurrentCount}");
                result = new ObjectResult("Server busy, try again later");
                result.StatusCode = (int)System.Net.HttpStatusCode.TooManyRequests;
            }

            return result;
        }

        static readonly SemaphoreSlim LocalLimit = new SemaphoreSlim(10);
    }
}
