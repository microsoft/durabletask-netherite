// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TokenCredentialDF
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Collections.Generic;
    using System.Net;

    /// <summary>
    /// A simple Http Trigger that is useful for testing whether the service has started correctly and can execute orchestrations.
    /// </summary>
    public static class Hello
    {

        [FunctionName(nameof(Hello))]
        public async static Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "hello")] HttpRequest req,
            [DurableClient] IDurableClient client)
        {
            try
            {
                string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence));
                TimeSpan timeout = TimeSpan.FromSeconds(30);
                return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
            }
            catch (Exception e)
            {
                return new ObjectResult($"exception: {e}") { StatusCode = (int)HttpStatusCode.InternalServerError };
            }         
        }

        [FunctionName(nameof(HelloSequence))]
        public static async Task<List<string>> HelloSequence([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var result = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(SayHello), "London")
            };
            return result;
        }

        [FunctionName(nameof(SayHello))]
        public static Task<string> SayHello([ActivityTrigger] IDurableActivityContext context)
        {
            return Task.FromResult(context.GetInput<string>());
        }
    }
}
