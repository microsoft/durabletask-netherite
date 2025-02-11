// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Tests;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "true")]
    public class CoreScenarios : IntegrationTestBase
    {
        public CoreScenarios(ITestOutputHelper output)
            : base(output)
        {
            TestConstants.ValidateEnvironment(requiresTransportSpec: true);
            this.AddFunctions(typeof(Functions));
        }

        static readonly TimeSpan defaultTimeout = TimeSpan.FromMinutes(1);

        [Fact]
        public Task HostCanStartAndStop()
        {
            return Common.WithTimeoutAsync(defaultTimeout, () =>
            {
                // Ensure (via logs) that the Durable extension is loaded
                IEnumerable<string> extensionLogs = this.GetExtensionLogs();
                Assert.NotEmpty(extensionLogs);

                // Ensure (via logs) that the Netherite provider correctly loaded.
                IEnumerable<string> providerLogs = this.GetProviderLogs();
                Assert.NotEmpty(providerLogs);
            });
        }

        [Fact]
        public Task CanRunActivitySequences()
        {
            return Common.WithTimeoutAsync(defaultTimeout, async () =>
            {
                DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.Sequence));
                Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
                Assert.Equal(10, (int)status.Output);
            });
        }

        [Fact]
        public Task CanPurgeAndListInstances()
        {
            return Common.WithTimeoutAsync(defaultTimeout, async () =>
            {
                await this.PurgeAllAsync();
                var results = await this.GetInstancesAsync(new OrchestrationStatusQueryCondition());  
                Assert.Empty(results);
            });
        }


        [Fact]
        public Task CanRunFanOutFanIn()
        {
            return Common.WithTimeoutAsync(defaultTimeout, async () =>
            {
                DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.FanOutFanIn));
                Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
                Assert.Equal(
                    expected: @"[""9"",""8"",""7"",""6"",""5"",""4"",""3"",""2"",""1"",""0""]",
                    actual: status.Output?.ToString(Formatting.None));
            });
        }

        [Fact]
        public Task CanOrchestrateEntities()
        {
            return Common.WithTimeoutAsync(defaultTimeout, async () =>
            {
                DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.OrchestrateCounterEntity));
                Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
                Assert.Equal(7, (int)status.Output);
            });
        }

        [Fact]
        public Task CanClientInteractWithEntities()
        {
            return Common.WithTimeoutAsync(defaultTimeout, async () =>
            {
                IDurableClient client = await this.GetDurableClientAsync();

                var entityId = new EntityId(nameof(Functions.Counter), Guid.NewGuid().ToString("N"));
                EntityStateResponse<int> result = await client.ReadEntityStateAsync<int>(entityId);
                Assert.False(result.EntityExists);

                await Task.WhenAll(
                    client.SignalEntityAsync(entityId, "incr"),
                    client.SignalEntityAsync(entityId, "incr"),
                    client.SignalEntityAsync(entityId, "incr"),
                    client.SignalEntityAsync(entityId, "add", 4));

                await Task.Delay(TimeSpan.FromSeconds(5));

                result = await client.ReadEntityStateAsync<int>(entityId);
                Assert.True(result.EntityExists);
                Assert.Equal(7, result.EntityState);
            });
        }

        [Fact]
        public Task CanOrchestrationInteractWithEntities()
        {
            return Common.WithTimeoutAsync(defaultTimeout, async () =>
            {
                DurableOrchestrationStatus status = await this.RunOrchestrationAsync(nameof(Functions.IncrementThenGet));
                Assert.Equal(OrchestrationRuntimeStatus.Completed, status.RuntimeStatus);
                Assert.Equal(1, (int)status.Output);
            });
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


            [FunctionName(nameof(OrchestrateCounterEntity))]
            public static async Task<int> OrchestrateCounterEntity(
                [OrchestrationTrigger] IDurableOrchestrationContext ctx)
            {
                var entityId = new EntityId(nameof(Counter), ctx.NewGuid().ToString("N"));
                ctx.SignalEntity(entityId, "incr");
                ctx.SignalEntity(entityId, "incr");
                ctx.SignalEntity(entityId, "incr");
                ctx.SignalEntity(entityId, "add", 4);

                using (await ctx.LockAsync(entityId))
                {
                    int result = await ctx.CallEntityAsync<int>(entityId, "get");
                    return result;
                }
            }

            [FunctionName(nameof(Counter))]
            public static void Counter([EntityTrigger] IDurableEntityContext ctx)
            {
                int current = ctx.GetState<int>();
                switch (ctx.OperationName)
                {
                    case "incr":
                        ctx.SetState(current + 1);
                        break;
                    case "add":
                        int amount = ctx.GetInput<int>();
                        ctx.SetState(current + amount);
                        break;
                    case "get":
                        ctx.Return(current);
                        break;
                    case "set":
                        amount = ctx.GetInput<int>();
                        ctx.SetState(amount);
                        break;
                    case "delete":
                        ctx.DeleteState();
                        break;
                    default:
                        throw new NotImplementedException("No such entity operation");
                }
            }

            [FunctionName(nameof(IncrementThenGet))]
            public static async Task<int> IncrementThenGet([OrchestrationTrigger] IDurableOrchestrationContext context)
            {
                // Key needs to be pseudo-random to avoid conflicts with multiple test runs.
                string key = context.NewGuid().ToString().Substring(0, 8);
                EntityId entityId = new EntityId(nameof(Counter), key);

                context.SignalEntity(entityId, "add", 1);

                // Invoking a sub-orchestration as a regression test for https://github.com/microsoft/durabletask-mssql/issues/146
                return await context.CallSubOrchestratorAsync<int>(nameof(GetEntityAsync), entityId);
            }

            [FunctionName(nameof(GetEntityAsync))]
            public static async Task<int> GetEntityAsync([OrchestrationTrigger] IDurableOrchestrationContext context)
            {
                EntityId entityId = context.GetInput<EntityId>();
                return await context.CallEntityAsync<int>(entityId, "get");
            }
        }
    }
}
