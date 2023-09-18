// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.HelloCities
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
    using System.Text;

    /// <summary>
    /// A simple microbenchmark orchestration that executes several simple "hello" activities in a sequence.
    /// </summary>
    public static partial class HelloSequence
    {
        [FunctionName(nameof(HelloSequence3))]
        public static async Task<List<string>> HelloSequence3([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var result = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "London")
            };

            return result;
        }

        [FunctionName(nameof(HelloSequence5))]
        public static async Task<List<string>> HelloSequence5([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "London"),
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "Amsterdam"),
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), "Mumbai")
            };

            return outputs;
        }

        [FunctionName(nameof(HelloSequenceN))]
        public static async Task HelloSequenceN([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            int numberSteps = context.GetInput<int>();
            for (int i = 0; i < numberSteps; i++)
            {
                await context.CallActivityAsync<string>(nameof(Activities.SayHello), $"City{i}");
            }
        }
    }
}
