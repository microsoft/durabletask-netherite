// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using Microsoft.Azure.Cosmos.Table;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "true")]
    public partial class ConcurrentTests : IDisposable
    {
        ITestOutputHelper outputHelper;
        readonly NetheriteOrchestrationServiceSettings settings;

        public ConcurrentTests(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            this.settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            this.settings.TestHooks.CacheDebugger = new Faster.CacheDebugger(this.settings.TestHooks);
            this.settings.FasterCacheSizeMB = 1;
            string timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff");
            this.settings.HubName = $"ConcurrentTests-{timestamp}";
        }

        public void Dispose()
        {
            this.outputHelper.WriteLine("CACHEDEBUGGER DUMP: --------------------------------------------------------------------------------------------------------------");
            foreach (var line in this.settings.TestHooks.CacheDebugger.Dump())
            {
                this.outputHelper.WriteLine(line);
            }
            this.outputHelper = null;
        }

        async Task WaitForCompletion(List<(string, Task)> tests, TimeSpan timeout)
        {
            var alldone = Task.WhenAll(tests.Select(x => x.Item2));

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            string errorInTestHooks = null;

            this.settings.TestHooks.OnError += (string message) =>
            {
                this.outputHelper?.WriteLine(message);
                errorInTestHooks ??= message;
            };

            while (!alldone.IsCompleted && errorInTestHooks == null)
            {
                string incomplete = string.Join(", ", tests.Where(x => !x.Item2.IsCompleted).Select(x => x.Item1));
                Trace.WriteLine($"TestProgress: Waiting for {incomplete}");

                if (stopwatch.Elapsed > timeout)
                {
                    throw new TimeoutException($"Some tests did not complete: {incomplete}");
                }

                // report progress every 15 seconds
                var checkAgain = Task.Delay(TimeSpan.FromSeconds(15));
                await Task.WhenAny(alldone, checkAgain);
            }

            Assert.Null(errorInTestHooks);
            await Task.WhenAll(alldone); // to propagate exceptions
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EachScenarioOnce(bool restrictMemory)
        {
            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(restrictMemory ? 10 : 5));
            using var fixture = await SingleHostFixture.StartNew(this.settings, useReplayChecker: true, restrictMemory ? (int?) 1 : null, TimeSpan.FromMinutes(5), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = scenarios.StartAllScenarios(includeTimers: !restrictMemory, includeLarge: true).ToList();
            await this.WaitForCompletion(tests, TimeSpan.FromMinutes(restrictMemory ? 10 : 5));
        }

        [Theory]
        [InlineData(false, false, 4)]
        [InlineData(false, true, 4)]
        [InlineData(true, false, 4)]
        [InlineData(true, true, 4)]
        [InlineData(false, false, 20)]
        [InlineData(false, true, 20)]
        [InlineData(true, false, 20)]
        [InlineData(true, true, 20)]
        public async Task ScaleSmallScenarios(bool useReplayChecker, bool restrictMemory, int multiplicity)
        {
            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes((restrictMemory ? 5 : 2) + multiplicity * (restrictMemory ? 0.5 : 0.1)));
            using var fixture = await SingleHostFixture.StartNew(this.settings, useReplayChecker, restrictMemory ? (int?)3 : null, TimeSpan.FromMinutes(5), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = new List<(string, Task)>();

            for (int i = 0; i < multiplicity; i++)
            {
                tests.AddRange(scenarios.StartAllScenarios(false, false));
            }

            await this.WaitForCompletion(tests, TimeSpan.FromMinutes((restrictMemory ? 10 : 5) + multiplicity * (restrictMemory ? 0.5 : 0.1)));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        [InlineData(8)]
        public async Task ReproHangingReads(int sequenceNumber)
        {
            // running a single test is usually not enough to repro, so we run the same test multiple times
            this.outputHelper.WriteLine($"starting test {sequenceNumber}");

            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(8));
            using var fixture = await SingleHostFixture.StartNew(this.settings, false, 1, TimeSpan.FromMinutes(5), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = new List<(string, Task)>();

            for (int i = 0; i < 30; i++)
            {
                tests.AddRange(scenarios.StartAllScenarios(false, false));
            }

            await this.WaitForCompletion(tests, TimeSpan.FromMinutes(20));
        }
    }
}
