namespace LoadGeneratorApp
{
    using System;
    // Copyright (c) Microsoft Corporation.
    // Licensed under the MIT License.

    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    //using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    public static class Callback
    {
        [FunctionName(nameof(Callback))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "node/{node}/request/{requestId}/callback")] HttpRequest req,
            [DurableClient] IDurableClient client,
            int node,
            Guid requestId,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var entityId = CallbackRouter.GetId(node);
            await client.SignalEntityAsync(entityId, "callback", (requestId, requestBody));
            return new OkResult();
        }
    }
}
