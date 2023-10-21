// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FanOutFanIn
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
    using System.Net;

    /// <summary>
    /// A simple microbenchmark orchestration that executes activities in parallel.
    /// </summary>
    public static class HttpTriggers
    {
        [FunctionName(nameof(FanOutFanIn))]
        public static async Task<IActionResult> RunFanOutFanIn(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "fanoutfanin/{count}")] HttpRequest req,
           int count,
           [DurableClient] IDurableClient client)
        {
            try
            {
                // start the orchestration
                string orchestrationInstanceId = await client.StartNewAsync(nameof(FanOutFanInOrchestration), null, count);

                // wait for it to complete and return the result
                var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(600));

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
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() }) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
