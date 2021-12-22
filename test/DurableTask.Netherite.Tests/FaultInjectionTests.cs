// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    public class FaultInjectionTests : IDisposable
    {
        ITestOutputHelper outputHelper;

        public FaultInjectionTests(ITestOutputHelper outputHelper) 
        {
            this.outputHelper = outputHelper;
        }

        public void Dispose()
        {
            this.outputHelper = null;
        }

        [Fact]
        public async Task HeckleStartup()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.FaultInjector = new Faster.FaultInjector();
            
            // inject faults with growing success runs until the partition has successfully started
            settings.FaultInjector.SetMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns);
            settings.FaultInjector.InjectOnStartup = true;
            using var fixture = await SingleHostFixture.StartNew(settings, (msg) => this.outputHelper.WriteLine(msg));
            await settings.FaultInjector.WaitForStartup(1);

            // for the rest of the test, do not inject any more faults
            settings.FaultInjector.SetMode(Faster.FaultInjector.InjectionMode.None);

            var client = await fixture.Host.StartOrchestrationAsync(typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task HeckleHelloCompletion()
        {
            // start the test normally without faults
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.FaultInjector = new Faster.FaultInjector();
            settings.FaultInjector.InjectOnStartup = false;
            using var fixture = await SingleHostFixture.StartNew(settings, (msg) => this.outputHelper.WriteLine(msg));
            await settings.FaultInjector.WaitForStartup(1);

            // issue a new request
            var client = await fixture.Host.StartOrchestrationAsync(typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");

            // inject faults with growing success for the rest of the test
            settings.FaultInjector.SetMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns);

            var status = await client.WaitForCompletionWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10));

            Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task HeckleHelloCreation()
        {
            // start the test normally without faults
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.FaultInjector = new Faster.FaultInjector();
            settings.FaultInjector.InjectOnStartup = false;
            using var fixture = await SingleHostFixture.StartNew(settings, (msg) => this.outputHelper.WriteLine(msg));
            await settings.FaultInjector.WaitForStartup(1);

            // inject faults with growing success while creating the hello orchestrator
            settings.FaultInjector.SetMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns);

            // issue a new request
            var client = await fixture.Host.StartOrchestrationWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10), typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");

            // for the rest of the test, do not inject any more faults
            settings.FaultInjector.SetMode(Faster.FaultInjector.InjectionMode.None);

            var status = await client.WaitForCompletionWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10));

            Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task HeckleHelloCreationAndCompletion()
        {
            // start the test normally without faults
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.FaultInjector = new Faster.FaultInjector();
            settings.FaultInjector.InjectOnStartup = false;
            using var fixture = await SingleHostFixture.StartNew(settings, (msg) => this.outputHelper.WriteLine(msg));
            await settings.FaultInjector.WaitForStartup(1);

            // inject faults with growing success for the rest of the test
            settings.FaultInjector.SetMode(Faster.FaultInjector.InjectionMode.IncrementSuccessRuns);

            // issue a new request and wait for it
            var client = await fixture.Host.StartOrchestrationWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10), typeof(ScenarioTests.Orchestrations.SayHelloWithActivity), "World");
            var status = await client.WaitForCompletionWithRetriesAsync(TimeSpan.FromSeconds(240), TimeSpan.FromSeconds(10));

            Assert.Equal(Core.OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }
    }
}