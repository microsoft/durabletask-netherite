// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Xunit;
    using Xunit.Abstractions;
    using System.Linq;
    using System.Collections.Generic;

    using TestTraceListener = DurableTask.Netherite.Tests.SingleHostFixture.TestTraceListener;
    using Orchestrations = DurableTask.Netherite.Tests.ScenarioTests.Orchestrations;
    using Microsoft.Extensions.Logging;
    using DurableTask.Netherite;
    using DurableTask.Core.History;

    // These tests are copied from AzureStorageScenarioTests
    [Collection("NetheriteTests")]
    public partial class QueryTests : IClassFixture<SingleHostFixture>, IDisposable
    {
        readonly SingleHostFixture fixture;
        readonly TestOrchestrationHost host;
        readonly Action<string> output;
        ITestOutputHelper outputHelper;

        public QueryTests(SingleHostFixture fixture, ITestOutputHelper outputHelper)
        {
            this.fixture = fixture;
            this.host = fixture.Host;
            this.outputHelper = outputHelper;
            this.output = (string message) =>
            {
                try
                {
                    this.outputHelper?.WriteLine(message);
                }
                catch (Exception)
                {
                }
            };

            this.output($"Running pre-test operations on {fixture.GetType().Name}.");

            this.fixture.SetOutput(this.output); 

            Assert.False(fixture.HasError(out var error), $"could not start test because of preceding test failure: {error}");

            // purge all instances prior to each test
            if (! this.host.PurgeAllAsync().Wait(TimeSpan.FromMinutes(3)))
            {
                throw new TimeoutException("timed out while purging instances before starting test");
            }
            this.output($"Completed pre-test operations on {fixture.GetType().Name}.");
        }

        public void Dispose() 
        {
            this.output($"Running post-test operations on {this.fixture.GetType().Name}.");

            Assert.False(this.fixture.HasError(out var error), $"detected test failure: {error}");


            // purge all instances after each test
            // this helps to catch "bad states" (e.g. hung workers) caused by the tests
            if (!this.host.PurgeAllAsync().Wait(TimeSpan.FromMinutes(3)))
            {
                throw new TimeoutException("timed out while purging instances after running test");
            }

            this.fixture.DumpCacheDebugger();

            this.output($"Completed post-test operations on {this.fixture.GetType().Name}.");
            this.outputHelper = null;
        }

        /// <summary>
        /// Ported from AzureStorageScenarioTests
        /// </summary>
        [Fact]
        public async Task QueryOrchestrationInstancesByDateRange()
        {
            const int numInstances = 3;
            string getPrefix(int ii) => $"@inst{ii}@__";

            // Start multiple orchestrations. Use 1-based to avoid confusion where we use explicit values in asserts.
            for (var ii = 1; ii <= numInstances; ++ii)
            {
                var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), $"world {ii}", $"{getPrefix(ii)}__{Guid.NewGuid()}");
                await Task.Delay(100);  // To ensure time separation for the date time range queries
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            }

            var results = await this.host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(numInstances, results.Count);
            for (var ii = 1; ii <= numInstances; ++ii)
            {
                Assert.NotNull(results.SingleOrDefault(r => r.Output == $"\"Hello, world {ii}!\""));
            }

            // Select the middle instance for the time range query.
            var middleInstance = results.SingleOrDefault(r => r.Output == $"\"Hello, world 2!\"");
            void assertIsMiddleInstance(IList<OrchestrationState> testStates)
            {
                Assert.Equal(1, testStates.Count);
                Assert.Equal(testStates[0].OrchestrationInstance.InstanceId, middleInstance.OrchestrationInstance.InstanceId);
            }

            assertIsMiddleInstance(await this.host.GetOrchestrationStateAsync(CreatedTimeFrom: middleInstance.CreatedTime,
                                                                            CreatedTimeTo: middleInstance.CreatedTime.AddMilliseconds(50)));
            assertIsMiddleInstance(await this.host.GetOrchestrationStateAsync(InstanceIdPrefix: getPrefix(2)));
        }

        /// <summary>
        /// Validate query functions.
        /// </summary>
        [Fact]
        public async Task QueryOrchestrationInstanceByRuntimeStatus()
        {
            // Reuse counter as it provides a wait for the actor to complete itself.
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
            Trace.TraceInformation($"Test progress: Counter is running.");

            // We should have one orchestration state
            var instanceStates = await this.host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(1, instanceStates.Count);

            var inProgressStatus = new[] { OrchestrationStatus.Running, OrchestrationStatus.ContinuedAsNew };
            var completedStatus = new[] { OrchestrationStatus.Completed };

            async Task assertCounts(int running, int completed)
            {
                var runningStates = await this.host.GetOrchestrationStateAsync(RuntimeStatus: inProgressStatus);
                Assert.Equal(running, runningStates.Count);
                var completedStates = await this.host.GetOrchestrationStateAsync(RuntimeStatus: completedStatus);
                Assert.Equal(completed, completedStates.Count);
            }

            // Make sure the client and instance are still running and didn't complete early (or fail).
            var status = await client.GetStatusAsync();
            Assert.NotNull(status);
            Assert.Contains(status.OrchestrationStatus, inProgressStatus);
            await assertCounts(1, 0);

            // The end message will cause the actor to complete itself.
            Trace.TraceInformation($"Test progress: Sending event to Counter.");
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpEnd);
            status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Trace.TraceInformation($"Test progress: Counter completed.");

            // The client and instance should be Completed
            Assert.NotNull(status);
            Assert.Contains(status.OrchestrationStatus, completedStatus);
            await assertCounts(0, 1);
        }

        [Fact]
        public async void NoInstancesCreated()
        {
            var instanceStates = await this.host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(0, instanceStates.Count);
        }


        [Fact]
        public async Task PurgeInstanceHistoryForTimePeriodDeletePartially()
        {
            DateTime startDateTime = DateTime.Now;
            string firstInstanceId = Guid.NewGuid().ToString();
            TestOrchestrationClient client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
            await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            DateTime endDateTime = DateTime.Now;
            await Task.Delay(5000);
            string secondInstanceId = Guid.NewGuid().ToString();
            client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
            await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            string thirdInstanceId = Guid.NewGuid().ToString();
            client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
            await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            IList<OrchestrationState> results = await this.host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(3, results.Count);
            Assert.Equal("\"Done\"", results[0].Output);
            Assert.Equal("\"Done\"", results[1].Output);
            Assert.Equal("\"Done\"", results[2].Output);

            List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
            Assert.True(firstHistoryEvents.Count > 0);

            List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
            Assert.True(secondHistoryEvents.Count > 0);

            List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
            Assert.True(secondHistoryEvents.Count > 0);

            IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
            Assert.Equal(1, firstOrchestrationStateList.Count);
            Assert.Equal(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

            IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
            Assert.Equal(1, secondOrchestrationStateList.Count);
            Assert.Equal(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

            IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
            Assert.Equal(1, thirdOrchestrationStateList.Count);
            Assert.Equal(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

            await client.PurgeInstanceHistoryByTimePeriod(startDateTime, endDateTime, new List<OrchestrationStatus> { OrchestrationStatus.Completed, OrchestrationStatus.Terminated, OrchestrationStatus.Failed, OrchestrationStatus.Running });

            List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
            Assert.Empty(firstHistoryEventsAfterPurging);

            List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
            Assert.True(secondHistoryEventsAfterPurging.Count > 0);

            List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
            Assert.True(thirdHistoryEventsAfterPurging.Count > 0);

            firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
            Assert.Equal(0, firstOrchestrationStateList.Count);
            Assert.Null(firstOrchestrationStateList.FirstOrDefault());

            secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
            Assert.Equal(1, secondOrchestrationStateList.Count);
            Assert.Equal(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

            thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
            Assert.Equal(1, thirdOrchestrationStateList.Count);
            Assert.Equal(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);
        }
    }

    [Collection("NetheriteTests")]
    public partial class NonFixtureQueryTests : IDisposable
    {
        readonly TestTraceListener traceListener;
        readonly ILoggerFactory loggerFactory;
        readonly XunitLoggerProvider provider;

        public NonFixtureQueryTests(ITestOutputHelper outputHelper)
        {
            Action<string> output = (string message) => outputHelper.WriteLine(message);
           
            TestConstants.ValidateEnvironment();
            this.traceListener = new TestTraceListener() { Output = output };
            this.loggerFactory = new LoggerFactory();
            this.provider = new XunitLoggerProvider(output);
            this.loggerFactory.AddProvider(this.provider);
            Trace.Listeners.Add(this.traceListener);
        }

        public void Dispose()
        {
            this.provider.Output = null;
            this.traceListener.Output = null;
            Trace.Listeners.Remove(this.traceListener);
        }

        /// <summary>
        /// This exercises what LinqPAD queries do.
        /// </summary>
        [Fact]
        public async void SingleServiceQuery()
        {
            Trace.WriteLine("Starting the orchestration service...");
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
            await service.CreateAsync(true);
            await service.StartAsync();
            Trace.WriteLine("Orchestration service is started.");

            var _ = await service.GetOrchestrationStateAsync();

            Trace.WriteLine("shutting down the orchestration service...");
            await service.StopAsync();
            Trace.WriteLine("Orchestration service is shut down.");
        }
    }
}
