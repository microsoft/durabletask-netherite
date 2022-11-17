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
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "false")]
    public partial class ConcurrentTestsMemory : IDisposable
    {
        ITestOutputHelper outputHelper;
        readonly NetheriteOrchestrationServiceSettings settings;

        public ConcurrentTestsMemory(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            TestConstants.ValidateEnvironment(requiresTransportSpec: false);
            this.settings = TestConstants.GetNetheriteOrchestrationServiceSettings(emulationSpec: "Memory");
        }

        public void Dispose()
        {
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

        [Fact]
        public async Task EachScenarioOnce()
        {
            using var fixture = await HostFixture.StartNew(this.settings, useCacheDebugger: false, useReplayChecker: false, null, TimeSpan.FromMinutes(5), (msg) => this.outputHelper?.WriteLine(msg));
            var scenarios = new ScenarioTests(fixture, this.outputHelper);

            var tests = scenarios.StartAllScenarios(includeTimers: true, includeLarge: true).ToList();
            await this.WaitForCompletion(tests, TimeSpan.FromMinutes(2));
        }
    }
}
