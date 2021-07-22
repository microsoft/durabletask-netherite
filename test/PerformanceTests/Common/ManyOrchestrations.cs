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

    /// <summary>
    /// Http triggers for starting, awaiting, counting, or purging large numbers of orchestration instances
    /// 
    /// Example invocations:
    ///     curl https://.../start -d HelloSequence.1000        launch 1000 HelloSequence instances, from the http trigger
    ///     curl https://.../start -d HelloSequence.10000.200   launch 10000 HelloSequence instances, in portions of 200, from launcher entities
    ///     curl https://.../start -d XYZ.10000.200/abc         launch 10000 XYZ instances, in portions of 200, with inputs abc
    ///     curl https://.../await -d 1000                      waits for the 1000 instances to complete
    ///     curl https://.../count -d 1000                      check the status of the 1000 instances and reports the (last completed - first started) time range
    ///     curl https://.../purge -d 1000                      purges the 1000 instances
    ///     curl https://.../query -d ""                        issues a query to check the status of all orchestrations
    ///     curl https://.../query -d Orch001                   issues a query to check the status of all orchestrations with an InstanceIdPrefix of "Orch001"
    ///     
    /// </summary>
    public static class ManyOrchestrations
    {
        [FunctionName(nameof(Start))]
        public static async Task<IActionResult> Start(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string parameters;
                string input;

                // extract parameters and input from the request
                int slash = requestBody.IndexOf('/');
                if (slash != -1)
                {
                    parameters = requestBody.Substring(0, slash);
                    input = requestBody.Substring(slash + 1);
                }
                else
                {
                    parameters = requestBody;
                    input = null;
                }

                // extract parameters
                int firstdot = parameters.IndexOf('.');
                int seconddot = parameters.LastIndexOf('.');
                string orchestrationName = parameters.Substring(0, firstdot);
                int numberOrchestrations;
                int? portionSize;

                if (firstdot == seconddot)
                {
                    numberOrchestrations = int.Parse(parameters.Substring(firstdot + 1));
                    portionSize = null;
                }
                else
                {
                    numberOrchestrations = int.Parse(parameters.Substring(firstdot + 1, seconddot - (firstdot + 1)));
                    portionSize = int.Parse(parameters.Substring(seconddot + 1));
                }

                if (!portionSize.HasValue || portionSize.Value == 0)
                {
                    log.LogWarning($"Starting {numberOrchestrations} instances of {orchestrationName} from within HttpTrigger...");

                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();

                    // start all the orchestrations
                    await Enumerable.Range(0, numberOrchestrations).ParallelForEachAsync(200, true, (iteration) =>
                    {
                        var orchestrationInstanceId = InstanceId(iteration);
                        log.LogInformation($"starting {orchestrationInstanceId}");
                        return client.StartNewAsync(orchestrationName, orchestrationInstanceId, input ?? iteration.ToString());
                    });

                    double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

                    string message = $"Started all {numberOrchestrations} orchestrations in {elapsedSeconds:F2}s.";

                    log.LogWarning($"Started all {numberOrchestrations} orchestrations in {elapsedSeconds:F2}s.");

                    return new OkObjectResult($"{message}\n");
                }
                else
                {
                    log.LogWarning($"Starting {numberOrchestrations} instances of {orchestrationName} via launcher entities...");
                    int pos = 0;
                    int launcher = 0;
                    var tasks = new List<Task>();
                    while (pos < numberOrchestrations)
                    {
                        int portion = Math.Min(portionSize.Value, (numberOrchestrations - pos));
                        var entityId = new EntityId(nameof(LauncherEntity), $"launcher{launcher / 100:D6}!{launcher % 100:D2}");
                        tasks.Add(client.SignalEntityAsync(entityId, nameof(LauncherEntity.Launch), (orchestrationName, portion, pos, input)));
                        pos += portion;
                        launcher++;
                    }
                    await Task.WhenAll(tasks);

                    return new OkObjectResult($"Signaled {launcher} entities for starting {numberOrchestrations} orchestrations.\n");
                }
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }

        [FunctionName(nameof(Await))]
        public static async Task<IActionResult> Await(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            int numberOrchestrations = int.Parse(requestBody);
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromMinutes(5);

            // wait for the specified number of orchestration instances to complete
            try
            {
                log.LogWarning($"Awaiting {numberOrchestrations} orchestration instances...");

                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();

                var tasks = new List<Task<bool>>();

                int completed = 0;

                // wait for all the orchestrations
                await Enumerable.Range(0, numberOrchestrations).ParallelForEachAsync(200, true, async (iteration) =>
                {
                    var orchestrationInstanceId = InstanceId(iteration);
                    IActionResult response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, deadline - DateTime.UtcNow);

                    if (response is ObjectResult objectResult
                        && objectResult.Value is HttpResponseMessage responseMessage
                        && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                        && responseMessage.Content is StringContent stringContent)
                    {
                        log.LogInformation($"{orchestrationInstanceId} completed");
                        Interlocked.Increment(ref completed);
                    }
                });

                return new OkObjectResult(
                    completed == numberOrchestrations 
                    ? $"all {numberOrchestrations} orchestration instances completed.\n" 
                    : $"only {completed}/{numberOrchestrations} orchestration instances completed.\n");             
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }

        [FunctionName(nameof(Count))]
        public static async Task<IActionResult> Count(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "count")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                int numberOrchestrations = int.Parse(requestBody);

                int completed = 0;
                int pending = 0;
                int running = 0;
                int other = 0;
                int notfound = 0;

                long earliestStart = long.MaxValue;
                long latestUpdate = 0;
                bool gotTimeRange = false;

                object lockForUpdate = new object();

                var stopwatch = Stopwatch.StartNew();

                var tasks = new List<Task<bool>>();

                log.LogWarning($"Checking the status of {numberOrchestrations} orchestration instances...");
                await Enumerable.Range(0, numberOrchestrations).ParallelForEachAsync(200, true, async (iteration) =>
                {
                    var orchestrationInstanceId = InstanceId(iteration);
                    var status = await client.GetStatusAsync(orchestrationInstanceId);

                    lock (lockForUpdate)
                    {
                        if (status == null)
                        {
                            notfound++;
                        }
                        else
                        {
                            earliestStart = Math.Min(earliestStart, status.CreatedTime.Ticks);
                            latestUpdate = Math.Max(latestUpdate, status.LastUpdatedTime.Ticks);
                            gotTimeRange = true;

                            if (status.RuntimeStatus == OrchestrationRuntimeStatus.Pending)
                            {
                                pending++;
                            }
                            else if (status.RuntimeStatus == OrchestrationRuntimeStatus.Running)
                            {
                                running++;
                            }
                            else if (status.RuntimeStatus == OrchestrationRuntimeStatus.Completed)
                            {
                                completed++;
                            }
                            else
                            {
                                other++;
                            }
                        }
                    }
                });

                stopwatch.Stop();
                double recordRangeSec = gotTimeRange
                    ? (new DateTime(latestUpdate) - new DateTime(earliestStart)).TotalSeconds
                    : 0;
                double querySec = stopwatch.ElapsedMilliseconds / 1000.0;

                var resultObject = new
                { 
                    completed,
                    running,
                    pending,
                    other,
                    notfound,
                    recordRangeSec,
                    querySec
                };

                return new OkObjectResult($"{JsonConvert.SerializeObject(resultObject)}\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }
       
        [FunctionName(nameof(Query))]
        public static async Task<IActionResult> Query(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "query")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string instanceIdPrefix = requestBody;  // TODO: parsable format for CreationTimeTo/From and RuntimeStatus (I=...;F=...;T=...;S=...)

                var queryCondition = new OrchestrationStatusQueryCondition()
                {
                    InstanceIdPrefix = instanceIdPrefix.Length > 0 ? instanceIdPrefix : OrchInstanceIdPrefix,
                };

                int completed = 0;
                int pending = 0;
                int running = 0;
                int other = 0;

                long earliestStart = long.MaxValue;
                long latestUpdate = 0;

                log.LogWarning($"Checking the status of orchestration instances starting with Id prefix {queryCondition.InstanceIdPrefix}...");

                var stopwatch = Stopwatch.StartNew();

                do
                {
                    OrchestrationStatusQueryResult result = await client.ListInstancesAsync(queryCondition, CancellationToken.None);
                    queryCondition.ContinuationToken = result.ContinuationToken;

                    foreach (var status in result.DurableOrchestrationState)
                    {
                        if (status.RuntimeStatus == OrchestrationRuntimeStatus.Pending)
                        {
                            pending++;
                        }
                        else if (status.RuntimeStatus == OrchestrationRuntimeStatus.Running)
                        {
                            running++;
                        }
                        else if (status.RuntimeStatus == OrchestrationRuntimeStatus.Completed)
                        {
                            completed++;
                        }
                        else
                        {
                            other++;
                        }

                        earliestStart = Math.Min(earliestStart, status.CreatedTime.Ticks);
                        latestUpdate = Math.Max(latestUpdate, status.LastUpdatedTime.Ticks);
                    }

                } while (queryCondition.ContinuationToken != null);

                stopwatch.Stop();
                double recordRangeSec = (completed + pending + running + other > 0)
                    ? (new DateTime(latestUpdate) - new DateTime(earliestStart)).TotalSeconds
                    : 0;
                double querySec = stopwatch.ElapsedMilliseconds / 1000.0;

                var resultObject = new
                { 
                    completed,
                    running,
                    pending,
                    other,
                    recordRangeSec,
                    querySec
                };

                return new OkObjectResult($"{JsonConvert.SerializeObject(resultObject)}\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }

        [FunctionName(nameof(Purge))]
        public static async Task<IActionResult> Purge(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            int numberOrchestrations = int.Parse(requestBody);
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromMinutes(5);

            // wait for the specified number of orchestration instances to complete
            try
            {
                log.LogWarning($"Purging {numberOrchestrations} orchestration instances...");

                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();

                var tasks = new List<Task<bool>>();

                int deleted = 0;

                // start all the orchestrations
                await Enumerable.Range(0, numberOrchestrations).ParallelForEachAsync(200, true, async (iteration) =>
                {
                    var orchestrationInstanceId = InstanceId(iteration);
                    var response = await client.PurgeInstanceHistoryAsync(orchestrationInstanceId);
                    
                    Interlocked.Add(ref deleted, response.InstancesDeleted);
                });

                return new OkObjectResult(
                    deleted == numberOrchestrations
                    ? $"all {numberOrchestrations} orchestration instances purged.\n"
                    : $"only {deleted}/{numberOrchestrations} orchestration instances purged.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(new { error = e.ToString() });
            }
        }

        // we can use this to run on a subset of the available partitions
        static readonly int? restrictedPlacement = null;

        const string OrchInstanceIdPrefix = "Orch";

        public static string InstanceId(int index)
        {
            if (restrictedPlacement == null)
            {
                return $"{OrchInstanceIdPrefix}{index:X5}";
            }
            else
            {
                return $"{OrchInstanceIdPrefix}{index:X5}!{(index % restrictedPlacement):D2}";
            }
        }
    }
}
