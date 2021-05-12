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
    using System.Linq;
    using System.Collections.Concurrent;
    using System.Threading;

    /// <summary>
    /// A simple microbenchmark orchestration that executes activities in parallel.
    /// </summary>
    public static class FanOutFanIn
    {
        [FunctionName(nameof(FanOutFanIn10))]
        public static Task FanOutFanIn10([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Run(context, 10);
        }

        [FunctionName(nameof(FanOutFanIn100))]
        public static Task FanOutFanIn100([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Run(context, 100);
        }

        [FunctionName(nameof(FanOutFanIn1000))]
        public static Task FanOutFanIn1000([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Run(context, 1000);
        }

        [FunctionName(nameof(FanOutFanIn10000))]
        public static Task FanOutFanIn10000([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Run(context, 10000);
        }

        [FunctionName(nameof(FanOutFanIn100000))]
        public static Task FanOutFanIn100000([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Run(context, 10000);
        }

        public static Task Run(IDurableOrchestrationContext context, int count)
        {
            Task[] tasks = new Task[count];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = context.CallActivityAsync<string>("SayHello", i.ToString("00000"));
            }
            return Task.WhenAll(tasks);
        }
    }
}
