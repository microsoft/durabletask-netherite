// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public static class HttpTrigger
    {
        [FunctionName(nameof(StartFixedRateScenario))]
        public static async Task<IActionResult> StartFixedRateScenario(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "fixedrate/{testname}")] HttpRequest req,
           string testname,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            string id = Guid.NewGuid().GetHashCode().ToString("X");
            log.LogWarning($"Starting {testname}/{id} scenario.");

            object jsonResult;

            try
            {
                string request = await new StreamReader(req.Body).ReadToEndAsync();
                FixedRateParameters parameters;

                try
                {
                    parameters = JsonConvert.DeserializeObject<FixedRateParameters>(request);
                }
                catch (JsonReaderException jre)
                {
                    return new JsonResult(new { error = jre.Message })
                    {
                        StatusCode = (int)HttpStatusCode.BadRequest,
                    };
                }

                var input = new FixedRateScenario.Input()
                {
                    Testname = testname,
                    ScenarioId = id,
                    Parameters = parameters,
                };



                await client.StartNewAsync(nameof(FixedRateScenario), id, input);

                jsonResult = new
                {
                    Testname = testname,
                    Scenario = id,
                    Status = "Running",
                };
            }
            catch (Exception e)
            {
                jsonResult = new
                {
                    Testname = testname,
                    Scenario = id,
                    Status = "InternalError",
                    Message = e.ToString(),
                };
            }

            log.LogWarning($"Returning result: {JsonConvert.SerializeObject(jsonResult, Formatting.Indented)}");
            return new JsonResult(jsonResult);
        }


        [FunctionName(nameof(WaitForFixedRateScenario))]
        public static async Task<IActionResult> WaitForFixedRateScenario(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "fixedrate/{testname}/{id}")] HttpRequest req,
           [DurableClient] IDurableClient client,
           string testname,
           string id,
           ILogger log)
        {
            object jsonResult;
           
            try
            {
                log.LogWarning($"Checking status of {testname}/{id}...");

                IActionResult response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, id, TimeSpan.FromMinutes(1));

                if (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                    && responseMessage.Content is StringContent stringContent)
                {
                    string content = await stringContent.ReadAsStringAsync();
                    JToken result = JToken.Parse(content);

                    log.LogWarning($"Returning result");

                    jsonResult = new
                    {
                        Testname = testname,
                        Scenario = id,
                        Status = "Completed",
                        Result = result,
                    };
                }
                else
                {
                    jsonResult = new
                    {
                        Testname = testname,
                        Scenario = id,
                        Status = "Running",
                    };
                }

            }
            catch (Exception e)
            {
                jsonResult = new
                    {
                        Testname = testname,
                        Scenario = id,
                        Status = "InternalError",
                        Message = e.ToString(),
                    };
            }

            log.LogWarning($"Returning status result: {JsonConvert.SerializeObject(jsonResult,Formatting.Indented)}");
            return new JsonResult(jsonResult);
        }
    }
}