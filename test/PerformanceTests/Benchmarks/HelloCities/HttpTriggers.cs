// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.HelloCities
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Linq;
    using System.Collections.Concurrent;
    using System.Threading;
    using Newtonsoft.Json.Linq;
    using System.Net.Http;

    /// <summary>
    /// A simple microbenchmark orchestration that some trivial activities in a sequence.
    /// </summary>
    public static class HttpTriggers
    {
        [FunctionName(nameof(HelloCities))]
        public static async Task<IActionResult> HelloCities3(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence.HelloSequence3));

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
        }

        [FunctionName(nameof(HelloCities5))]
        public static async Task<IActionResult> HelloCities5(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence.HelloSequence5));

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
        }

        [FunctionName(nameof(HelloCitiesN))]
        public static async Task<IActionResult> HelloCitiesN(
          [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "hellocities/{count}")] HttpRequest req,
          [DurableClient] IDurableClient client,
          int count,
          ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence.HelloSequenceN), null, count);

            // wait for it to complete and return the result
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(500));

            if (response is ObjectResult objectResult
                && objectResult.Value is HttpResponseMessage responseMessage
                && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                && responseMessage.Content is StringContent stringContent)
            {
                var state = await client.GetStatusAsync(orchestrationInstanceId, false, false, false);
                var elapsedSeconds = (state.LastUpdatedTime - state.CreatedTime).TotalSeconds;
                var throughput = count / elapsedSeconds;
                response = new OkObjectResult(new { elapsedSeconds, count, throughput });
            }

            return response;
        }
    }
}