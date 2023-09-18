// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FanOutFanIn
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
    using System.Linq;
    using System.Collections.Concurrent;
    using System.Threading;

    /// <summary>
    /// A simple microbenchmark orchestration that executes "hello" activities in parallel.
    /// </summary>
    public static class FanOutFanInOrchestration
    {
        [FunctionName(nameof(FanOutFanInOrchestration))]
        public static async Task<string> Run([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            int count = context.GetInput<int>();
            Task[] tasks = new Task[count];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = context.CallActivityAsync<string>("SayHello", i.ToString());
            }
            await Task.WhenAll(tasks);

            return $"{count} tasks completed.";
        }
    }
}
