// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Sequence
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    /// <summary>
    /// An orchestration that runs the sequence in DF style.
    /// </summary>
    public static class OrchestratedSequence
    {
        [FunctionName(nameof(OrchestratedSequence))]
        public static async Task<string> Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            Sequence.Input input = context.GetInput<Sequence.Input>();

            for (int i = 0; i < input.Length; i++)
            {
                input = await context.CallActivityAsync<Sequence.Input>(nameof(SequenceTask), input);
            }

            var result = $"orchestrated sequence {context.InstanceId} completed in {input.Duration}s";
            logger.LogWarning(result);
            return result;
        }

        [FunctionName(nameof(SequenceTask))]
        public static Task<Sequence.Input> SequenceTask(
            [ActivityTrigger] IDurableActivityContext context,
            ILogger logger)
        {
            Sequence.Input input = context.GetInput<Sequence.Input>();
            input = Sequence.RunTask(input, logger, context.InstanceId);
            return Task.FromResult(input);
        }
    }
}
