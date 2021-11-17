// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Sequence
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.Storage.Queue;

    /// <summary>
    /// An orchestration that runs the sequence using queue triggers.
    /// </summary>
    public static class QueueSequence
    {
        [Disable]
        [FunctionName(nameof(QueueSequence))]
        public static async Task<string> Run(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            Sequence.Input input = context.GetInput<Sequence.Input>();
            input.InstanceId = context.InstanceId;
            await context.CallActivityAsync(nameof(QueueSequenceTaskStart), input);
            input = await context.WaitForExternalEvent<Sequence.Input>("completed");
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
            Sequence.Input input = context.GetInput<Sequence.Input>();
            string content = JsonConvert.SerializeObject(input);
            await queue.CreateIfNotExistsAsync();
            await queue.AddMessageAsync(new CloudQueueMessage(content));
            logger.LogWarning($"queue sequence {context.InstanceId} started.");
        }

        [Disable]
        [FunctionName(nameof(QueueSequenceTask1))]
        public static async Task QueueSequenceTask1(
           [QueueTrigger("sequence")] string serialized,
           [Queue("sequence")] CloudQueue queue,
           [DurableClient] IDurableClient client,
           ILogger logger)
        {
            Sequence.Input input = JsonConvert.DeserializeObject<Sequence.Input>(serialized);

            logger.LogWarning($"queue sequence {input.InstanceId} step {input.Position} triggered.");

            input = Sequence.RunTask(input, logger, input.InstanceId);

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
    }
}
