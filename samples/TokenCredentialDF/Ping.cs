// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TokenCredentialDF
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
    using System.Net;

    /// <summary>
    /// A simple Http Trigger that is useful for testing whether the functions host has started correctly.
    /// </summary>
    public static class Ping
    {
        [FunctionName(nameof(Ping))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            [DurableClient(ConnectionName = "MyConnection")] IDurableClient client,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            log.LogInformation("C# HTTP trigger function processed a request.");
            var is64bit = Environment.Is64BitProcess;

            try
            {
                return new OkObjectResult($"Hello from {client} ({(is64bit ? "x64" : "x32")})\n");
            }
            catch (Exception e)
            {
                return new ObjectResult($"exception: {e}") { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
