// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace StandaloneClient
{
    using System;
    using System.Threading.Tasks;
    using System.Net;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Options;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask.ContextImplementations;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask.Options;
    using static StandaloneClient.Startup;

    public class Test
    {
        readonly ExternalClientFactory externalClientFactory;

        public Test(ExternalClientFactory externalClientFactory)
        {
            this.externalClientFactory = externalClientFactory;
        }

        [FunctionName(nameof(Test))]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req)
        {
            try
            {
                var client = this.externalClientFactory.GetClient();
                string orchestrationInstanceId = await client.StartNewAsync("HelloSequence");
                return new OkObjectResult($"client successfully started the instance {orchestrationInstanceId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult($"exception: {e}") { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
