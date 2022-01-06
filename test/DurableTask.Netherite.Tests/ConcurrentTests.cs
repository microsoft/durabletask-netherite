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
        }

        public void Dispose()
        {
            this.outputHelper = null;
        }

        async Task WaitForCompletion(List<(string, Task)> tests)
        {
            var alldone = Task.WhenAll(tests.Select(x => x.Item2));

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            TimeSpan timeout = TimeSpan.FromMinutes(5);

            while (!alldone.IsCompleted)
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

            await Task.WhenAll(alldone); // to propagate exceptions
        }

        [Fact]
        public async Task EachScenarioOnce()
        {
            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(1));
            using var fixture = await SingleHostFixture.StartNew(this.settings, false, TimeSpan.FromSeconds(30), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = scenarios.StartAllScenarios().ToList();
            await this.WaitForCompletion(tests);
        }


        [Fact]
        public async Task SmallOnesWithReplayCheck()
        {
            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(1));
            using var fixture = await SingleHostFixture.StartNew(this.settings, true, TimeSpan.FromSeconds(30), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = scenarios.StartAllScenarios(false, false).ToList();

            await this.WaitForCompletion(tests);
        }

        [Fact]
        public async Task SmallOnesTimesTwenty()
        {
            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(1));
            using var fixture = await SingleHostFixture.StartNew(this.settings, false, TimeSpan.FromSeconds(30), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = new List<(string, Task)>();
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));
            tests.AddRange(scenarios.StartAllScenarios(false, false));

            await this.WaitForCompletion(tests);
        }
    }
}
