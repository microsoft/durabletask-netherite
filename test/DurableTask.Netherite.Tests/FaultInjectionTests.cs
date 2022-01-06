// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Faster;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "false")]
    public class FaultInjectionTests : IDisposable
    {
        ITestOutputHelper outputHelper;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly FaultInjector faultInjector;

        public FaultInjectionTests(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            this.faultInjector = new Faster.FaultInjector();
            this.settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            this.settings.ResolvedTransportConnectionString = "MemoryF";
            this.settings.TestHooks.FaultInjector = this.faultInjector;
            this.settings.PartitionCount = 1; // default, used by most tests
            this.DisableCheckpoints();
        }

        public void Dispose()
        {
            this.outputHelper = null;
        }

        void DisableCheckpoints()
        {
            this.settings.TakeStateCheckpointWhenStoppingPartition = false;
            this.settings.MaxNumberBytesBetweenCheckpoints = 1000000000000000;
            this.settings.MaxNumberEventsBetweenCheckpoints = 10000000000;
            this.settings.IdleCheckpointFrequencyMs = 10000000000000;      
        }

        [Fact]
        public async Task InjectStartup()
        {
            SingleHostFixture fixture = null;

            // inject faults with growing success runs until the partition has successfully started
            using (this.faultInjector.WithMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns, injectDuringStartup: true))
            {
                fixture = await SingleHostFixture.StartNew(this.settings, true, TimeSpan.FromMinutes(2), (msg) => this.outputHelper.WriteLine(msg));
                await this.faultInjector.WaitForStartup(this.settings.PartitionCount, TimeSpan.FromMinutes(2));
            }

            var client = await fixture.Host.StartOrchestrationAsync(typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));

            fixture?.Dispose();
        }

        [Fact]
        public async Task InjectHelloCreation()
        {
            using (var fixture = await SingleHostFixture.StartNew(this.settings, true, TimeSpan.FromMinutes(1), (msg) => this.outputHelper?.WriteLine(msg)))
            {
                await this.faultInjector.WaitForStartup(this.settings.PartitionCount, TimeSpan.FromSeconds(30));

                TestOrchestrationClient client;

                // inject faults with growing success while creating the hello orchestrator
                using (this.faultInjector.WithMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns, injectDuringStartup: false))
                {
                    // issue a new request
                    client = await fixture.Host.StartOrchestrationWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10), typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");
                }

                var status = await client.WaitForCompletionWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10));

                Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.Equal("World", JToken.Parse(status?.Input));
                Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
            }
        }

        [Fact]
        public async Task InjectHelloCompletion()
        {
            using (var fixture = await SingleHostFixture.StartNew(this.settings, true, TimeSpan.FromMinutes(1), (msg) => this.outputHelper.WriteLine(msg)))
            {
                // do not start injecting until all partitions have started
                await this.faultInjector.WaitForStartup(this.settings.PartitionCount, TimeSpan.FromSeconds(30));

                // issue a new request
                var client = await fixture.Host.StartOrchestrationAsync(typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");

                // inject faults with growing success for the rest of the test
                using (this.faultInjector.WithMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns, injectDuringStartup: false))
                {
                    var status = await client.WaitForCompletionWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(30));

                    Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
                    Assert.Equal("World", JToken.Parse(status?.Input));
                    Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
                }
            }
        }
    }
}