// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.HelloCities
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A simple microbenchmark orchestration that executes several simple "hello" activities in a sequence.
    /// </summary>
    public static partial class SemaphoreTest
    {
        [FunctionName("OrchestrationWithSemaphore")]
        public static async Task<string> OrchestrationWithSemaphore([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            EntityId semaphore = new EntityId("SemaphoreEntity", "MySemaphoreInstance");
            Guid requestId = context.NewGuid();
            DateTime startTime = context.CurrentUtcDateTime;
            TimeSpan timeOut = TimeSpan.FromMinutes(5);
            try
            {
                while (true)
                {
                    if (await context.CallEntityAsync<bool>(semaphore, "TryAcquire", requestId))
                    {
                        break; // we have acquired the semaphore
                    }
                    if (context.CurrentUtcDateTime > startTime + timeOut)
                    {
                        throw new Exception("timed out while waiting for semaphore");               
                    }
                    else
                    {
                        await context.CreateTimer(context.CurrentUtcDateTime + TimeSpan.FromSeconds(1), CancellationToken.None);
                    }             
                }           
                await context.CallActivityAsync("ActivityThatRequiresSemaphore", null);

                return "Completed successfully.";
            }
            finally
            {
                context.SignalEntity(semaphore, "Release", requestId);
            }
        }

        [FunctionName("SemaphoreEntity")]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx) 
            => ctx.DispatchAsync<SemaphoreEntity>();

        public class SemaphoreEntity
        {
            public List<Guid> Requests { get; set; } = new List<Guid>();

            public int MaxCount { get; set; } = 50;

            public bool TryAcquire(Guid id)
            {
                int position = this.Requests.IndexOf(id);
                if (position == -1)
                {
                    this.Requests.Add(id);
                    position = this.Requests.Count - 1;
                }
                return (position < this.MaxCount);
            }
 
            public void Release(Guid id)
            {
                this.Requests.Remove(id);
            }
        }

        [FunctionName("ActivityThatRequiresSemaphore")]
        public static Task ActivityThatRequiresSemaphore([ActivityTrigger] IDurableActivityContext context, ILogger logger)
        {
            logger.LogInformation("Entering");
            Thread.Sleep(100);
            logger.LogInformation("Exiting");
            return Task.CompletedTask;
        }
    }
}
