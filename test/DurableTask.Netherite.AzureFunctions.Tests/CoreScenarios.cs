// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Tests;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;

    public class CoreScenarios : IntegrationTestBase
    {
        public CoreScenarios(ITestOutputHelper output)
            : base(output)
        {
            TestConstants.ValidateEnvironment();
            this.AddFunctions(typeof(Functions));
        }

        [Fact]
        public void HostCanStartAndStop()
        {
            // Ensure (via logs) that the Durable extension is loaded
            IEnumerable<string> extensionLogs = this.GetExtensionLogs();
            Assert.NotEmpty(extensionLogs);

            // Ensure (via logs) that the Netherite provider correctly loaded.
            IEnumerable<string> providerLogs = this.GetProviderLogs();
            Assert.NotEmpty(providerLogs);
        }

        [Fact]
        public async Task CanRunActivitySequences()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.Sequence));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(10, (int)status.Output);
        }

        [Fact]
        public async Task CanRunFanOutFanIn()
        {
            DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.FanOutFanIn));
            Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
            Assert.Equal(
                expected: @"[""9"",""8"",""7"",""6"",""5"",""4"",""3"",""2"",""1"",""0""]",
                actual: status.Output?.ToString(Formatting.None));
        }

        static class Functions
        {
            [FunctionName(nameof(Sequence))]
            public static async Task<int> Sequence(
                [OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                int value = 0;
                for (int i = 0; i < 10; i++)
                {
                    value = await ctx.CallActivityAsync<int>(nameof(PlusOne), value);
                }

                return value;
            }

            [FunctionName(nameof(PlusOne))]
            public static int PlusOne([ActivityTrigger] int input) => input + 1;

            [FunctionName(nameof(FanOutFanIn))]
            public static async Task<string[]> FanOutFanIn(
                [OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                var tasks = new List<Task<string>>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(ctx.CallActivityAsync<string>(nameof(IntToString), i));
                }

                string[] results = await Task.WhenAll(tasks);
                Array.Sort(results);
                Array.Reverse(results);
                return results;
            }

            [FunctionName(nameof(IntToString))]
            public static string IntToString([ActivityTrigger] int input) => input.ToString();
        }
    }
}
