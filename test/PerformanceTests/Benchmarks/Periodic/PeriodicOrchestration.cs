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
            (int iterations, double minutes) = context.GetInput<(int,double)>();
            
            for (int i = 0; i < iterations; i++)
            {
                DateTime fireAt = context.CurrentUtcDateTime + TimeSpan.FromMinutes(minutes);

                if (!context.IsReplaying)
                {
                    log.LogWarning("{instanceId}: periodic timer: starting iteration {iteration}, to fire at {fireAt}", context.InstanceId, i, fireAt);
                }

                await context.CreateTimer(fireAt, CancellationToken.None);

                if (!context.IsReplaying)
                {
                    log.LogWarning("{instanceId}: periodic timer: iteration {iteration} fired at {accuracyMs:F2}ms relative to deadline", context.InstanceId, i, (UtcNowWithNoWarning() - fireAt).TotalMilliseconds);
                }
            };
        }

        static readonly Func<DateTime> UtcNowWithNoWarning = () => DateTime.UtcNow;      
    }
}