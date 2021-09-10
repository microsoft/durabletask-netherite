// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
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
    using System.Diagnostics;
    using System.Threading;
    using System.Net.Http;
    using System.Net;

    public static class Queries
    {
        [FunctionName(nameof(PagedQuery))]
        public static async Task<JsonResult> PagedQuery(
          [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "pagedquery")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            try
            {
                var queryCondition = new OrchestrationStatusQueryCondition()
                {                   
                    PageSize = 100,
                };

                try
                {
                    var parameters = req.GetQueryParameterDictionary();
                    {
                        if (parameters.TryGetValue("runtimeStatus", out string val))
                        {
                            parameters.Remove("runtimeStatus");
                            queryCondition.RuntimeStatus = val
                                .Split(",")
                                .Select(s => (OrchestrationRuntimeStatus)Enum.Parse(typeof(OrchestrationRuntimeStatus), s))
                                .ToList();
                        }
                    }
                    {
                        if (parameters.TryGetValue("createdTimeFrom", out string val))
                        {
                            parameters.Remove("createdTimeFrom");
                            queryCondition.CreatedTimeFrom = DateTime.Parse(val);
                        }
                    }
                    {
                        if (parameters.TryGetValue("createdTimeTo", out string val))
                        {
                            parameters.Remove("createdTimeTo");
                            queryCondition.CreatedTimeTo = DateTime.Parse(val);
                        }
                    }
                    {
                        if (parameters.TryGetValue("pageSize", out string val))
                        {
                            parameters.Remove("pageSize");
                            queryCondition.PageSize = int.Parse(val);
                        }
                    }
                    {
                        if (parameters.TryGetValue("instanceIdPrefix", out string val))
                        {
                            parameters.Remove("instanceIdPrefix");
                            queryCondition.InstanceIdPrefix = val;
                        }
                    }
                    {
                        if (parameters.TryGetValue("showInput", out string val))
                        {
                            parameters.Remove("showInput");
                            queryCondition.ShowInput = bool.Parse(val);
                        }
                    }
                    if (parameters.Count > 0)
                    {
                        throw new ArgumentException($"invalid parameter: {parameters.First().Key}");
                    }
                } 
                catch(Exception e)
                {
                    return new JsonResult(new
                    {
                        message = e.Message,
                    })
                    {
                        StatusCode = (int)HttpStatusCode.BadRequest,
                    };
                }

                int records = 0;
                int completed = 0;
                int inputchars = 0;
                int pages = 0;

                log.LogWarning($"Querying orchestration instances...");

                var stopwatch = Stopwatch.StartNew();

                do
                {
                    pages++;
                    OrchestrationStatusQueryResult result = await client.ListInstancesAsync(queryCondition, CancellationToken.None);
                    queryCondition.ContinuationToken = result.ContinuationToken;

                    foreach (var status in result.DurableOrchestrationState)
                    {
                        records++;

                        if (status.RuntimeStatus == OrchestrationRuntimeStatus.Completed)
                        {
                            completed++;
                        }
 
                        if (status.Input != null)
                        {
                            inputchars += status.Input.ToString().Length;
                        }
                    }

                } while (queryCondition.ContinuationToken != null);

                stopwatch.Stop();
                double querySec = stopwatch.ElapsedMilliseconds / 1000.0;

                return new JsonResult(new
                { 
                    records,
                    completed,
                    inputchars,
                    pages,
                    querySec,
                    throughput = records > 0 ? (records/querySec).ToString("F2") : "n/a",
                });   
            }
            catch (Exception e)
            {
                return new JsonResult(new 
                {
                    error = e.ToString() 
                })
                {
                    StatusCode = (int)HttpStatusCode.InternalServerError,
                };
            }
        }
    }
}
