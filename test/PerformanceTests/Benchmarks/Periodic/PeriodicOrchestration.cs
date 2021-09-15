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
    /// A simple microbenchmark orchestration that some trivial activities in a sequence.
    /// </summary>
    public static class PeriodicOrchestration
    { 
        [FunctionName(nameof(PeriodicOrchestration))]
        public static async Task Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            for (int i = 0; i < 5; i++)
            {
                DateTime fireAt = context.CurrentUtcDateTime + TimeSpan.FromSeconds(60);

                if (!context.IsReplaying)
                {
                    log.LogWarning($"{context.InstanceId}: starting timer for iteration {i}, to fire at {fireAt}");
                }

                await context.CreateTimer(fireAt, CancellationToken.None);

                if (!context.IsReplaying)
                {
                    log.LogWarning($"{context.InstanceId}: timer for iteration {i} fired at {(UtcNowWithNoWarning() - fireAt).TotalMilliseconds:F2}ms relative to deadline");
                }
            };
        }

        static readonly Func<DateTime> UtcNowWithNoWarning = () => DateTime.UtcNow;      
    }
}