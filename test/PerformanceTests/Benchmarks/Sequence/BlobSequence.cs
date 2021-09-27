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
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;

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
            input.InstanceId = context.InstanceId;
            await context.CallActivityAsync(nameof(SequenceTaskStart), input);
            input = await context.WaitForExternalEvent<Sequence.Input>("completed");
            var result = $"blob sequence {context.InstanceId} completed in {input.Duration}s\n";
            logger.LogWarning(result);
            return result;
        }

        [Disable]
        [FunctionName(nameof(SequenceTaskStart))]
        public static async Task SequenceTaskStart(
            [ActivityTrigger] IDurableActivityContext context,
            [Blob("blobtriggers/blobs")] CloudBlobDirectory directory,
            ILogger logger)
        {
            Sequence.Input input = context.GetInput<Sequence.Input>();
            await directory.Container.CreateIfNotExistsAsync();
            string content = JsonConvert.SerializeObject(input);
            var blob = directory.GetBlockBlobReference(Guid.NewGuid().ToString());
            await blob.UploadTextAsync(content);
            logger.LogWarning($"blob sequence {context.InstanceId} started.");
        }

        [Disable]
        [FunctionName(nameof(BlobSequenceTask1))]
        public static async Task BlobSequenceTask1(
           [BlobTrigger("blobtriggers/blobs/{blobname}")] Stream inputStream,
           [Blob("blobtriggers/blobs")] CloudBlobDirectory directory,
           [DurableClient] IDurableClient client,
           string blobname,
           ILogger logger)
        {
            Sequence.Input input;

            using (var streamReader = new StreamReader(inputStream))
                input = JsonConvert.DeserializeObject<Sequence.Input>(await streamReader.ReadToEndAsync());

            logger.LogWarning($"blob sequence {input.InstanceId} step {input.Position} triggered.");

            input = Sequence.RunTask(input, logger, input.InstanceId);

            if (input.Position < input.Length)
            {
                // write the blob to trigger the next task
                string content = JsonConvert.SerializeObject(input);
                var blob = directory.GetBlockBlobReference(Guid.NewGuid().ToString());
                await blob.UploadTextAsync(content);
            }
            else
            {
                // notify the waiting orchestrator
                await client.RaiseEventAsync(input.InstanceId, "completed", input);
            }
        }
    }
}
