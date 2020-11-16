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
    using Dynamitey;
    using System.Security.Cryptography;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// A microbenchmark that runs a sequence of tasks. The point is to compare sequence construction using blob-triggers
    /// with an orchestrator doing the same thing.
    /// 
    /// The blob and queue triggers are disabled by default. Uncomment the [Disable] attribute before compilation to use them.
    /// </summary>
    public static class Sequence
    {
        public class Input
        {
            //  For length 3 we would have 3 tasks chained, in this type of sequence:
            //  (createData) --transfer-data--> (doWork) --transfer-data--> (doWork)
            //  The duration is measured from before (createData) to after last (doWork)

            // parameters
            public int Length { get; set; }
            public int WorkExponent { get; set; }
            public int DataExponent { get; set; }

            // data
            public byte[] Data { get; set; }
            public int Position { get; set; }

            public string InstanceId { get; set; }
            public DateTime StartTime { get; set; } // first task starting
            public double Duration { get; set; } // last task ending

            public int DoWork()
            {
                long iterations = 1;
                for (int i = 0; i < this.WorkExponent; i++)
                {
                    iterations *= 10;
                }
                int collisionCount = 0;
                for (long i = 0; i < iterations; i++)
                {
                    if (i.GetHashCode() == 0)
                    {
                        collisionCount++;
                    }
                }
                return collisionCount;
            }

            public byte[] CreateData()
            {
                int numBytes = 1;
                for (int i = 0; i < this.DataExponent; i++)
                {
                    numBytes *= 10;
                }
                var bytes = new byte[numBytes];
                (new Random()).NextBytes(bytes);
                return bytes;
            }
        }

        static Input RunTask(Input input, ILogger logger, string instanceId)
        {
            if (input.Data == null)
            {
                input.StartTime = DateTime.UtcNow;
                input.Data = input.CreateData();
                input.Position = 0;
                logger.LogWarning($"{instanceId} Producing {input.Data.Length} bytes");
            }
            else
            {
                logger.LogWarning($"{instanceId} Doing work");
                int ignoredResult = input.DoWork();
                input.Position++;
                input.Duration = (DateTime.UtcNow - input.StartTime).TotalSeconds;
                logger.LogWarning($"{instanceId} Passing along {input.Data.Length} bytes");
            }
            return input;
        }

        //---- http trigger

        [Disable]
        [FunctionName(nameof(RunTriggerSequence))]
        public static async Task<IActionResult> RunTriggerSequence(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "RunTriggerSequence")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(200);
            string orchestrationInstanceId = await client.StartNewAsync(nameof(TriggeredSequence), null, new Input()
            {
                Length = 10,
                WorkExponent = 0, 
                DataExponent = 0,
            });
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
            return response;
        }

        [Disable]
        [FunctionName(nameof(RunQueueSequence))]
        public static async Task<IActionResult> RunQueueSequence(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "RunQueueSequence")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(200);
            string orchestrationInstanceId = await client.StartNewAsync(nameof(QueueSequence), null, new Input()
            {
                Length = 10,
                WorkExponent = 0,
                DataExponent = 0,
            });
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
            return response;
        }

        //-------------- orchestrated sequence

        [FunctionName(nameof(OrchestratedSequence))]
        public static async Task<string> OrchestratedSequence([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            Input input = context.GetInput<Input>();

            for (int i = 0; i < input.Length; i++)
            {
                input = await context.CallActivityAsync<Input>(nameof(SequenceTask), input);
            }

            var result = $"orchestrated sequence {context.InstanceId} completed in {input.Duration}s";
            logger.LogWarning(result);
            return result;
        }

        [FunctionName(nameof(SequenceTask))]
        public static Task<Input> SequenceTask(
            [ActivityTrigger] IDurableActivityContext context, 
            ILogger logger)
        {
            Input input = context.GetInput<Input>();
            input = RunTask(input, logger, context.InstanceId);
            return Task.FromResult(input);
        }

        //-------------- queue sequence

        [Disable]
        [FunctionName(nameof(QueueSequence))]
        public static async Task<string> QueueSequence(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            Input input = context.GetInput<Input>();
            input.InstanceId = context.InstanceId;
            await context.CallActivityAsync(nameof(QueueSequenceTaskStart), input);
            input = await context.WaitForExternalEvent<Input>("completed");
            var result = $"queue sequence {context.InstanceId} completed in {input.Duration}s\n";
            logger.LogWarning(result);
            return result;
        }

        [Disable]
        [FunctionName(nameof(QueueSequenceTaskStart))]
        public static async Task QueueSequenceTaskStart(
            [ActivityTrigger] IDurableActivityContext context,
            [Queue("sequence")] CloudQueue queue,
            ILogger logger)
        {
            Input input = context.GetInput<Input>();
            string content = JsonConvert.SerializeObject(input);
            await queue.CreateIfNotExistsAsync();
            await queue.AddMessageAsync(new CloudQueueMessage(content));
        }

        [Disable]
        [FunctionName(nameof(QueueSequenceTask1))]
        public static async Task QueueSequenceTask1(
           [QueueTrigger("sequence")] string serialized,
           [Queue("sequence")] CloudQueue queue,
           [DurableClient] IDurableClient client,
           ILogger logger)
        {
            Input input = JsonConvert.DeserializeObject<Input>(serialized);

            input = RunTask(input, logger, input.InstanceId);

            if (input.Position < input.Length)
            {
                // write to the queue to trigger the next task
                string content = JsonConvert.SerializeObject(input);
                await queue.AddMessageAsync(new CloudQueueMessage(content));
            }
            else
            {
                // notify the waiting orchestrator
                await client.RaiseEventAsync(input.InstanceId, "completed", input);
            }
        }

        //-------------- triggered sequence

        [Disable]
        [FunctionName(nameof(TriggeredSequence))]
        public static async Task<string> TriggeredSequence(
            [OrchestrationTrigger] IDurableOrchestrationContext context, 
            ILogger logger)
        {
            Input input = context.GetInput<Input>();
            await context.CallActivityAsync(nameof(SequenceTaskStart), input);
            input = await context.WaitForExternalEvent<Input>("completed");
            var result = $"triggered sequence {context.InstanceId} completed in {input.Duration}s\n";
            logger.LogWarning(result);
            return result;
        }

        [Disable]
        [FunctionName(nameof(SequenceTaskStart))]
        public static async Task SequenceTaskStart(
            [ActivityTrigger] IDurableActivityContext context,
            [Blob("tasks/step/0")] Microsoft.WindowsAzure.Storage.Blob.CloudBlobDirectory directory,
            ILogger logger)
        {
            Input input = context.GetInput<Input>();
            var blob = directory.GetBlockBlobReference(context.InstanceId);
            string content = JsonConvert.SerializeObject(input);
            await blob.Container.CreateIfNotExistsAsync();
            await blob.UploadTextAsync(content);
        }

        [Disable]
        [FunctionName(nameof(TriggeredSequenceTask1))]
        public static async Task TriggeredSequenceTask1(
           [BlobTrigger("tasks/step/{iteration}/{instanceId}")] Stream inputStream,
           [Blob("tasks/step")] Microsoft.WindowsAzure.Storage.Blob.CloudBlobDirectory directory,
           [DurableClient] IDurableClient client,
           string instanceId,
           ILogger logger)
        {
            Input input;

            using (var streamReader = new StreamReader(inputStream))
                input = JsonConvert.DeserializeObject<Input>(await streamReader.ReadToEndAsync());

            input = RunTask(input, logger, instanceId);

            if (input.Position < input.Length)
            {
                // write the blob to trigger the next task
                string content = JsonConvert.SerializeObject(input);
                var blob = directory.GetBlockBlobReference($"{input.Position}/{instanceId}");
                await blob.UploadTextAsync(content);
            }
            else
            {
                // notify the waiting orchestrator
                await client.RaiseEventAsync(instanceId, "completed", input);
            }
        }
    }
}
