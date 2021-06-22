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
    using Newtonsoft.Json.Linq;
    using System.Linq;
    using System.Threading;

    /// <summary>
    /// A workflow for database obfuscation, adapted from the step-function 
    /// implementation at https://github.com/FINRAOS/maskopy
    /// </summary>
    public static class MaskopyCaseStudy
    {
        [FunctionName(nameof(MaskopyCaseStudy))]
        public static async Task<IActionResult> RunLambda(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "maskopy")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            var testname = Util.MakeTestName(req);
            //bool waitForCompletion = true;

            try
            {
                log.LogWarning($"Starting {testname} ...");

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

                dynamic inputJson = JToken.Parse(requestBody);

                int numberSequential = inputJson.sequential;

                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();

                var sequentialOrchestrationInstanceId = $"MaskopyMaster:sequential-{testname}";
                var sequentialInput = new Tuple<string, bool>(requestBody, false);
                await client.StartNewAsync(nameof(MaskopyOrchestrations), sequentialOrchestrationInstanceId, sequentialInput);
                log.LogInformation($"{testname} started {sequentialOrchestrationInstanceId} with input {sequentialInput.Item1}");
                var sequentialResponse = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, sequentialOrchestrationInstanceId, TimeSpan.FromMinutes(20));

                //var parallelOrchestrationInstanceId = $"ImageRecognitionMaster:parallel-{testname}";
                //var parallelInput = new Tuple<string, bool>(requestBody, true);
                //await client.StartNewAsync(nameof(ImageRecognitionOrchestrations), parallelOrchestrationInstanceId, parallelInput);
                //log.LogInformation($"{testname} started {parallelOrchestrationInstanceId} with input {parallelInput.Item1}");
                //var parallelResponse = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, parallelOrchestrationInstanceId, TimeSpan.FromMinutes(10));

                stopwatch.Stop();

                log.LogWarning($"Completed {testname}.");

                object resultObject = new
                {
                    testname,
                    elapsedSeconds = stopwatch.Elapsed.TotalSeconds
                };

                string resultString = $"{JsonConvert.SerializeObject(resultObject, Formatting.None)}\n";

                return new OkObjectResult(resultString);
            }
            catch (Exception e)
            {
                return new ObjectResult(
                    new
                    {
                        testname,
                        error = e.ToString(),
                    });
            }
        }

        public static async Task<string> MakeHttpRequestSync(string name, string stringUri, string input, IDurableOrchestrationContext context, ILogger log)
        {
            var uri = new Uri($"{stringUri}");
            if (!context.IsReplaying) log.LogInformation("Calling {name} with input: {input} at uri: {uri}", name, input, stringUri);
            var result = await context.CallHttpAsync(System.Net.Http.HttpMethod.Post, uri, input);
            if (result.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception($"incorrect result returned by {name}: {result.Content}");
            }
            string output = result.Content;
            if (!context.IsReplaying) log.LogInformation("{name} returned output: {output}", name, output);
            return output;
        }

        public static Task<DurableHttpResponse> MakeHttpRequest(string name, string stringUri, string input, IDurableOrchestrationContext context, ILogger log)
        {
            var uri = new Uri($"{stringUri}");
            if (!context.IsReplaying) log.LogInformation("Calling {name} with input: {input} at uri: {uri}", name, input, stringUri);
            return context.CallHttpAsync(System.Net.Http.HttpMethod.Post, uri, input);
        }

        public static string GetHttpResult(string name, DurableHttpResponse response, IDurableOrchestrationContext context, ILogger log)
        {
            if (response.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception($"incorrect result returned by {name}: {response.Content}");
            }
            string output = response.Content;
            if (!context.IsReplaying) log.LogInformation("{name} returned output: {output}", name, output);
            return output;
        }

        [FunctionName(nameof(MaskopyOrchestrations))]
        public static async Task<string> MaskopyOrchestrations([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var input = context.GetInput<Tuple<string, bool>>();
            var isParallel = input.Item2;
            var inputJsonString = input.Item1;
            dynamic inputJson = JToken.Parse(inputJsonString);
            int numberExecutions = isParallel ? inputJson.parallel : inputJson.sequential;

            // Create a base object that will be used for each separate call
            JObject baseObject = new JObject(inputJson);
            baseObject.Remove("sequential");

            var tasks = new List<Task>();
            for (int i = 0; i < numberExecutions; i ++)
            {
                string orchestrationInputSerialized = inputJsonString;

                // Call the orchestration 
                //var orchestrationInstanceId = $"ImageRecognition:{i}:{isParallel}-{objectID}";
                var task = context.CallSubOrchestratorAsync<string>(nameof(MaskopyOrchestration), orchestrationInputSerialized);
                
                if (isParallel)
                {
                    tasks.Add(task);
                }
                else 
                {
                    var _ = await task;
                }
            }

            if (isParallel)
            {
                await Task.WhenAll(tasks);
            }
            return "Orchestration was completed successfully!";
        }


        /// <summary>
        /// It is an orchestration that executes the image Recognition orchestration
        /// </summary>
        [FunctionName(nameof(MaskopyOrchestration))]
        public static async Task<string> MaskopyOrchestration([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var inputJsonString = context.GetInput<string>();
            dynamic inputJson = JToken.Parse(inputJsonString);
            string s3Bucket = inputJson.s3Bucket;
            string s3Key = inputJson.s3Key;
            string objectID = inputJson.objectID;

            try
            {
                // Execute MASKOPY-00-AuthorizeUser
                string userAuthorizedJsonString = await MakeHttpRequestSync("MASKOPY-00-AuthorizeUser", (string)inputJson.MASKOPY00AuthorizeUserURI, inputJsonString, context, log);

                // Execute MASKOPY-01-UseExistingSnapshot
                string createdSnapshotsJsonString = await MakeHttpRequestSync("MASKOPY-01-UseExistingSnapshot", (string)inputJson.MASKOPY01UseExistingSnapshotURI, inputJsonString, context, log);
                dynamic createdSnapshotsJson = JToken.Parse(createdSnapshotsJsonString);
                inputJson.CreatedSnapshots = createdSnapshotsJson;

                // Wait until the snapshot is completed by calling the CheckForSnapshotCompletion Lambda
                do
                {
                    // Wait
                    DateTime retryWait = context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(5));
                    await context.CreateTimer(retryWait, CancellationToken.None);

                    // Execute MASKOPY-02-CheckForSnapshotCompletion
                    string checkForSnapshotCompletionInput = JsonConvert.SerializeObject(inputJson);
                    string snapshotsAvailableJsonString = await MakeHttpRequestSync("MASKOPY-02-CheckForSnapshotCompletion", (string)inputJson.MASKOPY02CheckForSnapshotCompletionURI, checkForSnapshotCompletionInput, context, log);
                    bool snapshotsAvailable = JToken.Parse(snapshotsAvailableJsonString).ToObject<bool>();
                    inputJson.SnapshotsAvailable = snapshotsAvailable;
                }
                while (inputJson.SnapshotsAvailable != true);

                // Execute MASKOPY-03-ShareSnapshots
                string shareSnapshotsInput = JsonConvert.SerializeObject(inputJson);
                string sharedSnapshotsJsonString = await MakeHttpRequestSync("MASKOPY-03-ShareSnapshots", (string)inputJson.MASKOPY03ShareSnapshotsURI, shareSnapshotsInput, context, log);
                dynamic sharedSnapshotsJson = JToken.Parse(sharedSnapshotsJsonString);
                inputJson.SharedSnapshots = sharedSnapshotsJson;

                // Execute MASKOPY-04-CopySharedDBSnapshots
                string copySharedDBSnapshotsInput = JsonConvert.SerializeObject(inputJson);
                string createdDestinationSnapshotsJsonString = await MakeHttpRequestSync("MASKOPY-04-CopySharedDBSnapshots", (string)inputJson.MASKOPY04CopySharedDBSnapshotsURI, copySharedDBSnapshotsInput, context, log);
                dynamic createdDestinationSnapshotsJson = JToken.Parse(createdDestinationSnapshotsJsonString);
                inputJson.CreatedDestinationSnapshots = createdDestinationSnapshotsJson;

                // Wait until the snapshot is completed by calling the CheckForDestinationSnapshotCompletion Lambda
                do
                {
                    // Wait
                    DateTime retryWait = context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(5));
                    await context.CreateTimer(retryWait, CancellationToken.None);

                    // Execute MASKOPY-05-CheckForDestinationSnapshotCompletion
                    string checkForDestinationSnapshotCompletionInput = JsonConvert.SerializeObject(inputJson);
                    string DestinationSnapshotsAvailableJsonString = await MakeHttpRequestSync("MASKOPY-05-CheckForDestinationSnapshotCompletion", (string)inputJson.MASKOPY05CheckForDestinationSnapshotCompletionURI, checkForDestinationSnapshotCompletionInput, context, log);
                    bool DestinationSnapshotsAvailable = JToken.Parse(DestinationSnapshotsAvailableJsonString).ToObject<bool>();
                    inputJson.DestinationSnapshotsAvailable = DestinationSnapshotsAvailable;
                }
                while (inputJson.DestinationSnapshotsAvailable != true);

                // Execute MASKOPY-06-RestoreDatabases
                string restoreDatabasesInput = JsonConvert.SerializeObject(inputJson);
                string DestinationRestoredDatabasesJsonString = await MakeHttpRequestSync("MASKOPY-06-RestoreDatabases", (string)inputJson.MASKOPY06RestoreDatabasesURI, restoreDatabasesInput, context, log);
                dynamic DestinationRestoredDatabasesJson = JToken.Parse(DestinationRestoredDatabasesJsonString);
                inputJson.DestinationRestoredDatabases = DestinationRestoredDatabasesJson;

                // Wait until the database is restored
                do
                {
                    // Wait
                    DateTime retryWait = context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(5));
                    await context.CreateTimer(retryWait, CancellationToken.None);

                    // Execute MASKOPY-07-CheckForRestoreCompletion
                    string checkForRestoreCompletionInput = JsonConvert.SerializeObject(inputJson);
                    string DestinationRestoredDatabasesCompleteJsonString = await MakeHttpRequestSync("MASKOPY-07-CheckForRestoreCompletion", (string)inputJson.MASKOPY07CheckForRestoreCompletionURI, checkForRestoreCompletionInput, context, log);
                    bool DestinationRestoredDatabasesComplete = JToken.Parse(DestinationRestoredDatabasesCompleteJsonString).ToObject<bool>();
                    inputJson.DestinationRestoredDatabasesComplete = DestinationRestoredDatabasesComplete;
                }
                while (inputJson.DestinationRestoredDatabasesComplete != true);

                // Execute MASKOPY-08a-CreateFargate
                string CreateFargateInput = JsonConvert.SerializeObject(inputJson);
                string fargateJsonString = await MakeHttpRequestSync("MASKOPY-08a-CreateFargate", (string)inputJson.MASKOPY08aCreateFargateURI, CreateFargateInput, context, log);
                dynamic fargateJson = JToken.Parse(fargateJsonString);
                inputJson.fargate = fargateJson;

                // TODO: Run the fargate task MASKOPY-08a-RunFargateTask
                string RunFargateTaskInput = fargateJsonString;
                string ECSRunTaskJsonString = await MakeHttpRequestSync("MASKOPY-08a-RunFargateTask", (string)inputJson.MASKOPY08aRunFargateTaskURI, RunFargateTaskInput, context, log);
                dynamic ECSRunTaskJson = JToken.Parse(ECSRunTaskJsonString);
                inputJson.ECSRunTask = ECSRunTaskJson;

                // Wait until the fargate task is done
                do
                {
                    // Wait
                    DateTime retryWait = context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(5));
                    await context.CreateTimer(retryWait, CancellationToken.None);

                    // Execute MASKOPY-08b-WaitForFargateTask
                    string WaitForFargateTaskInput = ECSRunTaskJsonString;
                    string FargateTaskCompleteJsonString = await MakeHttpRequestSync("MASKOPY-08b-WaitForFargateTask", (string)inputJson.MASKOPY08bWaitForFargateTaskURI, WaitForFargateTaskInput, context, log);
                    bool FargateTaskComplete = JToken.Parse(FargateTaskCompleteJsonString).ToObject<bool>();
                    inputJson.FargateTaskComplete = FargateTaskComplete;
                }
                while (inputJson.FargateTaskComplete != true);
                // Maybe we actually need to return the description

                // Execute MASKOPY-09-TakeSnapshot
                string TakeSnapshotInput = JsonConvert.SerializeObject(inputJson);
                string CreatedFinalSnapshotsJsonString = await MakeHttpRequestSync("MASKOPY-09-TakeSnapshot", (string)inputJson.MASKOPY09TakeSnapshotURI, TakeSnapshotInput, context, log);
                dynamic CreatedFinalSnapshotsJson = JToken.Parse(CreatedFinalSnapshotsJsonString);
                inputJson.CreatedFinalSnapshots = CreatedFinalSnapshotsJson;

                // Wait until the final snapshot is done
                do
                {
                    // Wait
                    DateTime retryWait = context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(5));
                    await context.CreateTimer(retryWait, CancellationToken.None);

                    // Execute MASKOPY-10-CheckFinalSnapshotAvailability
                    string CheckFinalSnapshotAvailabilityInput = JsonConvert.SerializeObject(inputJson);
                    string FinalSnapshotAvailableJsonString = await MakeHttpRequestSync("MASKOPY-10-CheckFinalSnapshotAvailability", (string)inputJson.MASKOPY10CheckFinalSnapshotAvailabilityURI, CheckFinalSnapshotAvailabilityInput, context, log);
                    bool FinalSnapshotAvailable = JToken.Parse(FinalSnapshotAvailableJsonString).ToObject<bool>();
                    inputJson.FinalSnapshotAvailable = FinalSnapshotAvailable;
                }
                while (inputJson.FinalSnapshotAvailable != true);
            }
            catch
            {
                // Catch errors by calling ErrorHandling and Cleanup
                string ErrorHandlingAndCleanupInput = JsonConvert.SerializeObject(inputJson);
                string DeletedResourcesJsonString = await MakeHttpRequestSync("MASKOPY-ErrorHandlingAndCleanup", (string)inputJson.MASKOPYErrorHandlingAndCleanupURI, ErrorHandlingAndCleanupInput, context, log);
                JToken DeletedResourcesJson = JToken.Parse(DeletedResourcesJsonString);
                inputJson.DeletedResources = DeletedResourcesJson;

                return $"Orchestration Failed!";
            }


            // Execute MASKOPY-11-CleanupAndTagging
            string CleanupAndTaggingInput = JsonConvert.SerializeObject(inputJson);
            string CleanupAndTaggingJsonString = await MakeHttpRequestSync("MASKOPY-11-CleanupAndTagging", (string)inputJson.MASKOPY11CleanupAndTaggingURI, CleanupAndTaggingInput, context, log);
            JToken CleanupAndTaggingJson = JToken.Parse(CleanupAndTaggingJsonString);
            inputJson.CleanupAndTagging = CleanupAndTaggingJson;

            // TODO: Send a message to SQS in both success or failure

            if (CleanupAndTaggingJson.First().SelectToken("Success").ToObject<bool>() == true)
            {
                return $"Successfully executed orchestration.";
            }
            else
            {
                return $"Orchestration Failed!";
            }
        }
    }
}
