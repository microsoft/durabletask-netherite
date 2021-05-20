// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Sequence
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
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// An orchestration that runs the sequence using blob triggers.
    /// </summary>
    public static class BlobSequence
    {
        [Disable]
        [FunctionName(nameof(BlobSequence))]
        public static async Task<string> Run(
            [OrchestrationTrigger] IDurableOrchestrationContext context, 
            ILogger logger)
        {
            Sequence.Input input = context.GetInput<Sequence.Input>();
            await context.CallActivityAsync(nameof(SequenceTaskStart), input);
            input = await context.WaitForExternalEvent<Sequence.Input>("completed");
            var result = $"blob-triggered sequence {context.InstanceId} completed in {input.Duration}s\n";
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
            Sequence.Input input = context.GetInput<Sequence.Input>();
            var blob = directory.GetBlockBlobReference(context.InstanceId);
            string content = JsonConvert.SerializeObject(input);
            await blob.Container.CreateIfNotExistsAsync();
            await blob.UploadTextAsync(content);
        }

        [Disable]
        [FunctionName(nameof(BlobSequenceTask1))]
        public static async Task BlobSequenceTask1(
           [BlobTrigger("tasks/step/{iteration}/{instanceId}")] Stream inputStream,
           [Blob("tasks/step")] Microsoft.WindowsAzure.Storage.Blob.CloudBlobDirectory directory,
           [DurableClient] IDurableClient client,
           string instanceId,
           ILogger logger)
        {
            Sequence.Input input;

            using (var streamReader = new StreamReader(inputStream))
                input = JsonConvert.DeserializeObject<Sequence.Input>(await streamReader.ReadToEndAsync());

            input = Sequence.RunTask(input, logger, instanceId);

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
