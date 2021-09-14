// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Periodic
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
    using Newtonsoft.Json;
    using System.Net;

    /// <summary>
    /// A simple microbenchmark orchestration that some trivial activities in a sequence.
    /// </summary>
    public static class Periodic
    {
        [FunctionName(nameof(Periodic))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "periodic/{instanceId}/start")] HttpRequest req,
           string instanceId,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            PeriodicOrchestration.Input input;

            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                input = JsonConvert.DeserializeObject<PeriodicOrchestration.Input>(requestBody);
            }
            catch (Exception e)
            {
                return new ObjectResult(e.Message) { StatusCode = (int)HttpStatusCode.BadRequest };
            }

            try
            {
                // start the orchestration
                string orchestrationInstanceId = await client.StartNewAsync(nameof(PeriodicOrchestration), instanceId, input);
                return new OkObjectResult($"periodic orchestration {instanceId} was started.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}