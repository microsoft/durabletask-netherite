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
    [Trait("AnyTransport", "false")]
    public partial class ConcurrentTestsFaster : IDisposable
    {
        ITestOutputHelper outputHelper;
        readonly NetheriteOrchestrationServiceSettings settings;

        public ConcurrentTestsFaster(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            TestConstants.ValidateEnvironment(requiresTransportSpec: false);
            this.settings = TestConstants.GetNetheriteOrchestrationServiceSettings(emulationSpec: "SingleHost");
            string timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff");
            this.settings.HubName = $"ConcurrentTests-{Guid.NewGuid().ToString("n")}";
        }

        public void Dispose()
        {
            if (this.settings.TestHooks.CacheDebugger != null)
            {
                this.outputHelper.WriteLine("CACHEDEBUGGER DUMP: --------------------------------------------------------------------------------------------------------------");
                foreach (var line in this.settings.TestHooks.CacheDebugger.Dump())
                {
                    this.outputHelper.WriteLine(line);
                }
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
            var orchestrationTimeout = TimeSpan.FromMinutes(restrictMemory ? 10 : 5);
            var startupTimeout = TimeSpan.FromMinutes(TransportConnectionString.IsPseudoConnectionString(this.settings.ResolvedTransportConnectionString) ? 1 : 3.5);
            var shutDownTimeout = TimeSpan.FromMinutes(TransportConnectionString.IsPseudoConnectionString(this.settings.ResolvedTransportConnectionString) ? 0.1 : 3);
            var totalTimeout = startupTimeout + orchestrationTimeout + shutDownTimeout;

            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(restrictMemory ? 10 : 5));

            async Task RunAsync()
            {
                Trace.WriteLine($"TestProgress: Started RunAsync");

                using (var fixture = await HostFixture.StartNew(
                    this.settings,
                    useCacheDebugger: true,
                    useReplayChecker: false,
                    restrictMemory ? (int?)0 : null,
                    startupTimeout,
                    (msg) => this.outputHelper?.WriteLine(msg)))
                {
                    var scenarios = new ScenarioTests(fixture, this.outputHelper);

                    var tests = new List<(string, Task)>();

                    foreach ((string name, Task task) in scenarios.StartAllScenarios(includeTimers: true, includeLarge: true))
                    {
                        Trace.WriteLine($"TestProgress: Adding {name}");
                        tests.Add((name, task));
                    }

                    await this.WaitForCompletion(tests, orchestrationTimeout);

                    Trace.WriteLine($"TestProgress: Shutting Down");
                }

                Trace.WriteLine($"TestProgress: Completed RunAsync");
            }

            var task = Task.Run(RunAsync);
            var timeoutTask = Task.Delay(totalTimeout);
            await Task.WhenAny(task, timeoutTask);
            Assert.True(task.IsCompleted);
            await task;
        }

        [Theory]
        [InlineData(false, false, 4)]
        [InlineData(false, true, 4)]
        [InlineData(true, false, 4)]
        [InlineData(true, true, 4)]
        [InlineData(false, false, 20)]
        [InlineData(false, true, 20)]
        [InlineData(true, false, 20)]
        public async Task ScaleSmallScenarios(bool useReplayChecker, bool restrictMemory, int multiplicity)
        {
            var orchestrationTimeout = TimeSpan.FromMinutes((restrictMemory ? 10 : 5) + multiplicity * (restrictMemory ? 0.5 : 0.1));
            var startupTimeout = TimeSpan.FromMinutes(TransportConnectionString.IsPseudoConnectionString(this.settings.ResolvedTransportConnectionString) ? 1 : 3.5);
            var testTimeout = orchestrationTimeout + TimeSpan.FromMinutes(multiplicity * 0.2);
            var shutDownTimeout = TimeSpan.FromMinutes(TransportConnectionString.IsPseudoConnectionString(this.settings.ResolvedTransportConnectionString) ? 0.1 : 3);
            var totalTimeout = startupTimeout + testTimeout + shutDownTimeout;

            using var _ = TestOrchestrationClient.WithExtraTime(orchestrationTimeout);

            async Task RunAsync()
            {
                Trace.WriteLine($"TestProgress: Started RunAsync");

                using (var fixture = await HostFixture.StartNew(
                    this.settings,
                    true,
                    useReplayChecker,
                    restrictMemory ? (int?)0 : null,
                    startupTimeout,
                    (msg) => this.outputHelper?.WriteLine(msg))) 
                {
                    var scenarios = new ScenarioTests(fixture, this.outputHelper);

                    var tests = new List<(string, Task)>();

                    for (int i = 0; i < multiplicity; i++)
                    {
                        foreach((string name, Task task) in scenarios.StartAllScenarios(false, false))
                        {
                            Trace.WriteLine($"TestProgress: Adding {name}");
                            tests.Add((name,task));
                        }
                    }

                    await this.WaitForCompletion(tests, testTimeout);

                    Trace.WriteLine($"TestProgress: Shutting Down");
                }

                Trace.WriteLine($"TestProgress: Completed RunAsync");
            }

            var task = Task.Run(RunAsync);
            var timeoutTask = Task.Delay(totalTimeout);
            await Task.WhenAny(task, timeoutTask);
            Assert.True(task.IsCompleted);
            await task;
        }

        [Theory]
        [InlineData(1)]
        //[InlineData(2)]
        //[InlineData(3)]
        //[InlineData(4)]
        //[InlineData(5)]
        //[InlineData(6)]
        //[InlineData(7)]
        //[InlineData(8)]
        public async Task ReproHangingReads(int sequenceNumber)
        {
            // running a single test is usually not enough to repro, so we run the same test multiple times
            this.outputHelper.WriteLine($"starting test {sequenceNumber}");

            // disable checkpoints since they are not needed to trigger the bug
            this.settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 * 1024 * 1024;
            this.settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
            this.settings.IdleCheckpointFrequencyMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;

            this.settings.PartitionCount = 4;


            using var _ = TestOrchestrationClient.WithExtraTime(TimeSpan.FromMinutes(3));
            using var fixture = await HostFixture.StartNew(this.settings, true, false, 0, TimeSpan.FromMinutes(5), (msg) => this.outputHelper?.WriteLine(msg));

            this.settings.TestHooks.CacheDebugger.EnableSizeChecking = false;

            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = new List<(string, Task)>();

            for (int i = 0; i < 20; i++)
            {
                tests.AddRange(scenarios.StartAllScenarios(false, false));
            }

            await this.WaitForCompletion(tests, TimeSpan.FromMinutes(10));
        }
    }
}
