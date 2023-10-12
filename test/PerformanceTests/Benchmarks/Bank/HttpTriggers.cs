// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Bank
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
    using System.Net;

    /// <summary>
    /// A microbenchmark using durable entities for bank accounts, and an orchestration with a critical section for transferring
    /// currency between two accounts.
    /// </summary>
    public static class HttpTriggers
    {
        [FunctionName(nameof(Bank))]
        public static async Task<IActionResult> Bank(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "bank/{index}")] HttpRequest req,
            [DurableClient] IDurableClient client,
            int index,
            ILogger log)
        {
            try
            {
                log.LogWarning($"Starting BankTransaction({index})...");
                string instanceId = await client.StartNewAsync(nameof(BankTransaction), null, index);
                return  await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, TimeSpan.FromMinutes(1)); 
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
