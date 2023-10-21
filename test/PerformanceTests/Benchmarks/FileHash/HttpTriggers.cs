// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FileHash
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using System.Net.Http;
    using Newtonsoft.Json.Linq;
    using System.Net;

    public static class HttpTriggers
    {
        /// <summary>
        /// Http trigger to start a file hash and wait for the result.
        /// 
        /// </summary>
        [FunctionName(nameof(FileHash))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "FileHash")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            try
            {
                var startTime = DateTime.UtcNow;

                // get the number of files to hash
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numFiles = int.Parse(requestBody);
                log.LogWarning($"Received request to hash {numFiles} files");

                // start the orchestration
                string orchestrationInstanceId = await client.StartNewAsync(nameof(FileOrchestration), null, numFiles);

                // wait for it to complete and return the result
                var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(600));

                if (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                    && responseMessage.Content is StringContent stringContent)
                {
                    var state = await client.GetStatusAsync(orchestrationInstanceId, false, false, false);
                    var wordsHashed = (double)JToken.Parse(await stringContent.ReadAsStringAsync()); // millions of words
                    var elapsedSeconds = (state.LastUpdatedTime - state.CreatedTime).TotalSeconds;
                    var size = (double) wordsHashed;
                    var throughput = wordsHashed / elapsedSeconds;
                    response = new OkObjectResult(new { numFiles, wordsHashed, elapsedSeconds, size, throughput });
                }

                return response;
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() }) { StatusCode = (int) HttpStatusCode.InternalServerError };
            }
        }
    }
}
