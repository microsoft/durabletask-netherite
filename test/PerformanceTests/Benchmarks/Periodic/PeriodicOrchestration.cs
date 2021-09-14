// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Periodic
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Threading;

    /// <summary>
    /// A simple microbenchmark orchestration that executes a fixed-time activity within a timer loop
    /// </summary>
    public static class PeriodicOrchestration
    {
        public struct Input
        {
            public int Count;
            public TimeSpan Period;
            public TimeSpan? Duration;
        }

        [FunctionName(nameof(PeriodicOrchestration))]
        public static async Task Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var input = context.GetInput<Input>();
            DateTime nextFireAt = context.CurrentUtcDateTime;

            if (!context.IsReplaying)
            {
                log.LogWarning($"{context.InstanceId}: started at {nextFireAt:r}. count={input.Count} period={input.Period} duration={input.Duration?.ToString() ?? "none"}");
            }

            for (int i = 0; i < input.Count; i++)
            {
                if (i > 0)
                {
                    nextFireAt = nextFireAt + input.Period;

                    if (!context.IsReplaying)
                    {
                        log.LogWarning($"{context.InstanceId}: starting timer for iteration {i}, to fire at {nextFireAt:r}");
                    }

                    await context.CreateTimer(nextFireAt, CancellationToken.None);

                    if (!context.IsReplaying)
                    {
                        log.LogWarning($"{context.InstanceId}: timer for iteration {i} fired at {(UtcNowWithNoWarning() - nextFireAt).TotalMilliseconds:F2}ms relative to deadline");
                    }
                }

                if (input.Duration.HasValue)
                {
                    if (!context.IsReplaying)
                    {
                        log.LogWarning($"{context.InstanceId}: calling activity for iteration {i}");
                    }

                    await context.CallActivityAsync(nameof(TimedActivity), input.Duration.Value);
                }
            };

            if (!context.IsReplaying)
            {
                log.LogWarning($"{context.InstanceId}: completed");
            }
        }

        static readonly Func<DateTime> UtcNowWithNoWarning = () => DateTime.UtcNow;

        [FunctionName(nameof(TimedActivity))]
        public static Task TimedActivity([ActivityTrigger] IDurableActivityContext context)
        {
            var timespan = context.GetInput<TimeSpan>();
            return Task.Delay(timespan);
        }
    }
}