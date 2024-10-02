// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Data.Tables;
    using Castle.Core.Resource;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;

    // The majority of these tests were ported from AzureStorageScenarioTests in the azure/durabletask repository

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "true")]
    public partial class ScenarioTests : IClassFixture<HostFixture>, IDisposable
    {
        readonly HostFixture fixture;
        readonly TestOrchestrationHost host;
        readonly Action<string> output;
        ITestOutputHelper outputHelper;

        public ScenarioTests(HostFixture fixture, ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            this.output = (string message) => this.outputHelper?.WriteLine(message);

            this.output($"Running pre-test operations on {fixture.GetType().Name}.");

            this.fixture = fixture;
            this.host = fixture.Host;
            fixture.SetOutput(this.output);
            Assert.False(fixture.HasError(out var error), $"could not start test because of preceding test failure: {error}");

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

            Assert.Null(this.fixture.TestHooksError);
            this.fixture.DumpCacheDebugger();

            this.output($"Completed post-test operations on {this.fixture.GetType().Name}.");
            this.outputHelper = null;
        }

        public IEnumerable<(string, Task)> StartAllScenarios(bool includeTimers = true, bool includeLarge = true)
        {
            yield return ("HelloWorldOrchestration_Inline", this.HelloWorldOrchestration_Inline());
            yield return ("HelloWorldOrchestration_Activity", this.HelloWorldOrchestration_Activity());
            yield return ("SequentialOrchestration", this.SequentialOrchestration());
            yield return ("EventConversation", this.EventConversation());
            yield return ("AutoStart", this.AutoStart());
            yield return ("HelloWorldSubOrchestration_FanOutFanIn", this.HelloWorldOrchestration_Activity());
            yield return ("ContinueAsNewThenTimer", this.ContinueAsNewThenTimer());
            yield return ("ParallelOrchestration", this.ParallelOrchestration());
            yield return ("ActorOrchestration", this.ActorOrchestration());
            yield return ("TerminateOrchestration", this.TerminateOrchestration());
            yield return ("RecreateCompletedInstance", this.RecreateCompletedInstance());
            yield return ("RecreateCompletedInstanceWithSuborchestration", this.RecreateCompletedInstanceWithSuborchestration());
            yield return ("RecreateFailedInstance", this.RecreateFailedInstance());
            yield return ("RecreateTerminatedInstance", this.RecreateTerminatedInstance());
            yield return ("RecreateRunningInstance", this.RecreateRunningInstance());
            yield return ("HandledActivityException", this.HandledActivityException());
            yield return ("UnhandledOrchestrationException", this.UnhandledOrchestrationException());
            yield return ("UnhandledActivityException", this.UnhandledActivityException());
            yield return ("SmallTextMessagePayloads", this.SmallTextMessagePayloads());
            yield return ("DoubleFanOut", this.DoubleFanOut());

            if (includeTimers)
            {
                yield return ("TimerCancellation", this.TimerCancellation());
                yield return ("TimerExpiration", this.TimerExpiration());
            }

            if (includeLarge)
            {
                yield return ("LargeInputAndOutput", this.LargeInputAndOutput());
                yield return ("FanOutToTableStorage", this.FanOutToTableStorage());
                yield return ("LargeFanOutOrchestration", this.LargeFanOutOrchestration());
                yield return ("FanOutOrchestration_LargeHistoryBatches", this.FanOutOrchestration_LargeHistoryBatches());
            }
        }

        /// <summary>
        /// End-to-end test which validates a simple orchestrator function which doesn't call any activity functions.
        /// </summary>
        [Fact]
        public async Task HelloWorldOrchestration_Inline()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which runs a simple orchestrator function that calls a single activity function.
        /// </summary>
        [Fact]
        public async Task HelloWorldOrchestration_Activity()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloWithActivity), "World");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which runs a simple orchestrator function that calls a single activity function.
        /// </summary>
        [Fact]
        public async Task HelloWorldSubOrchestration_FanOutFanIn()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloFanOutFanIn), 10);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("00000,00001,00002,00003,00004,00005,00006,00007,00008,00009", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        [Fact]
        public async Task SequentialOrchestration()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 10);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(10, JToken.Parse(status?.Input));
            Assert.Equal(3628800, JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task ParentOfSequentialOrchestration()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.ParentOfFactorial), 10);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(10, JToken.Parse(status?.Input));
            Assert.Equal(3628800, JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task EventConversation()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(EventConversationOrchestration), "");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("OK", JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task AutoStart()
        {
            this.host.AddAutoStartOrchestrator(typeof(Orchestrations.AutoStartOrchestration.Responder));

            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.AutoStartOrchestration), "");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("OK", JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task ContinueAsNewThenTimer()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(ContinueAsNewThenTimerOrchestration), 0);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("OK", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates parallel function execution by enumerating all files in the current directory 
        /// in parallel and getting the sum total of all file sizes.
        /// </summary>
        [Fact]
        public async Task ParallelOrchestration()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.DiskUsage), Environment.CurrentDirectory);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(Environment.CurrentDirectory, JToken.Parse(status?.Input));
            Assert.True(long.Parse(status?.Output) > 0L);
        }

        [Fact]
        public async Task LargeFanOutOrchestration()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 1000);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(8));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        }

        [Fact]
        public async Task FanOutOrchestration_LargeHistoryBatches()
        {
            // This test creates history payloads that exceed the 4 MB limit imposed by Azure Storage
            // when 100 entities are uploaded at a time.
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.SemiLargePayloadFanOutFanIn), 90);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(120));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        }

        /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing a counter actor pattern.
        /// </summary>
        [Fact]
        public async Task ActorOrchestration()
        {
            int initialValue = 0;
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Counter), initialValue);

            // Need to wait for the instance to start before sending events to it.
            var state = await client.WaitForStartupAsync(TimeSpan.FromSeconds(30));
            Assert.NotNull(state);

            // Perform some operations
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpDecrement);
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpIncrement);
            await Task.Delay(2000);

            // Make sure it's still running and didn't complete early (or fail).
            state = await client.GetStateAsync();
            Assert.NotNull(state);
            Assert.True(
                state.OrchestrationStatus == OrchestrationStatus.Running ||
                state.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

            // The end message will cause the actor to complete itself.
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpEnd);

            state = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Completed, state?.OrchestrationStatus);
            Assert.Equal(3, JToken.Parse(state?.Output));

            // When using ContinueAsNew, the original input is discarded and replaced with the most recent state.
            Assert.NotEqual(initialValue, (int) JToken.Parse(state?.Input));
        }

        /// <summary>
        /// End-to-end test which validates the Terminate functionality.
        /// </summary>
        [Fact]
        public async Task TerminateOrchestration()
        {
            // Using the counter orchestration because it will wait indefinitely for input.
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);

            // Need to wait for the instance to start before we can terminate it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            await client.TerminateAsync("sayōnara");

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
            Assert.Equal("sayōnara", status?.Output);
        }

        /// <summary>
        /// End-to-end test which validates that a completed singleton instance can be recreated.
        /// </summary>
        [Fact]
        public async Task RecreateCompletedInstance()
        {
            string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "One",
                instanceId: singletonInstanceId);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("One", JToken.Parse(status?.Input));
            Assert.Equal("Hello, One!", JToken.Parse(status?.Output));

            client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "Two",
                instanceId: singletonInstanceId);
            status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("Two", JToken.Parse(status?.Input));
            Assert.Equal("Hello, Two!", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that a completed orchestration with suborchestration can be recreated.
        /// </summary>
        [Fact]
        public async Task RecreateCompletedInstanceWithSuborchestration()
        {
            string instanceId = $"Factorial{Guid.NewGuid():N}";

            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.ParentOfFactorial), 10, instanceId);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(10, JToken.Parse(status?.Input));
            Assert.Equal(3628800, JToken.Parse(status?.Output));

            client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.ParentOfFactorial), 10, instanceId);
            status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(10, JToken.Parse(status?.Input));
            Assert.Equal(3628800, JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that a completed singleton instance can be deduplicated.
        /// </summary>
        [Fact]
        public async Task DeduplicateCompletedInstance()
        {
            string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "One",
                instanceId: singletonInstanceId);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            var state1 = (await client.GetStateAsync(singletonInstanceId)).First();

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("One", JToken.Parse(status?.Input));
            Assert.Equal("Hello, One!", JToken.Parse(status?.Output));

            (bool created, _) = await this.host.StartDeduplicatedOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "Two",
                instanceId: singletonInstanceId,
                new OrchestrationStatus[] { OrchestrationStatus.Completed });

            Assert.False(created);

            var state2 = (await client.GetStateAsync(singletonInstanceId)).First();
            Assert.Equal(state1.OrchestrationInstance.ExecutionId, state2.OrchestrationInstance.ExecutionId);
        }

        /// <summary>
        /// End-to-end test which validates that a failed singleton instance can be recreated.
        /// </summary>
        [Fact]
        public async Task RecreateFailedInstance()
        {
            string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: null, // this will cause the orchestration to fail
                instanceId: singletonInstanceId);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Failed, status?.OrchestrationStatus);

            client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "NotNull",
                instanceId: singletonInstanceId);
            status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("Hello, NotNull!", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that a failed singleton instance can be deduplicated.
        /// </summary>
        [Fact]
        public async Task DeduplicateFailedInstance()
        {
            string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: null, // this will cause the orchestration to fail
                instanceId: singletonInstanceId);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            var state1 = (await client.GetStateAsync(singletonInstanceId)).First();

            Assert.Equal(OrchestrationStatus.Failed, status?.OrchestrationStatus);

            (bool created, _) = await this.host.StartDeduplicatedOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "Two",
                instanceId: singletonInstanceId,
                new OrchestrationStatus[] { OrchestrationStatus.Failed });

            Assert.False(created);

            var state2 = (await client.GetStateAsync(singletonInstanceId)).First();
            Assert.Equal(state1.OrchestrationInstance.ExecutionId, state2.OrchestrationInstance.ExecutionId);
        }

        /// <summary>
        /// End-to-end test which validates that a terminated orchestration can be recreated.
        /// </summary>
        [Fact]
        public async Task RecreateTerminatedInstance()
        {
            string singletonInstanceId = $"SingletonCounter_{Guid.NewGuid():N}";

            // Using the counter orchestration because it will wait indefinitely for input.
            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.Counter),
                input: -1,
                instanceId: singletonInstanceId);

            // Need to wait for the instance to start before we can terminate it.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            await client.TerminateAsync("sayōnara");

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
            var state1 = (await client.GetStateAsync(singletonInstanceId)).First();

            Assert.Equal(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
            Assert.Equal("-1", status?.Input);
            Assert.Equal("sayōnara", status?.Output);

            client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.Counter),
                input: 0,
                instanceId: singletonInstanceId);
            status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Running, status?.OrchestrationStatus);
            Assert.Equal("0", status?.Input);

            var state2 = (await client.GetStateAsync(singletonInstanceId)).First();
            Assert.NotEqual(state1.OrchestrationInstance.ExecutionId, state2.OrchestrationInstance.ExecutionId);
        }

        /// <summary>
        /// End-to-end test which validates that a terminated orchestration can be deduplicated.
        /// </summary>
        [Fact]
        public async Task DeduplicateTerminatedInstance()
        {
            string singletonInstanceId = $"SingletonCounter_{Guid.NewGuid():N}";

            // Using the counter orchestration because it will wait indefinitely for input.
            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.Counter),
                input: -1,
                instanceId: singletonInstanceId);

            // Need to wait for the instance to start before we can terminate it.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            await client.TerminateAsync("sayōnara");

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
            var state1 = (await client.GetStateAsync(singletonInstanceId)).First();

            Assert.Equal(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
            Assert.Equal("-1", status?.Input);
            Assert.Equal("sayōnara", status?.Output);

            (bool created, _) = await this.host.StartDeduplicatedOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "Two",
                instanceId: singletonInstanceId,
                new OrchestrationStatus[] { OrchestrationStatus.Terminated });

            Assert.False(created);

            var state2 = (await client.GetStateAsync(singletonInstanceId)).First();
            Assert.Equal(state1.OrchestrationInstance.ExecutionId, state2.OrchestrationInstance.ExecutionId);
        }


        /// <summary>
        /// End-to-end test which validates that a running orchestration can be recreated.
        /// </summary>
        [Fact]
        public async Task RecreateRunningInstance()
        {
            string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

            // Using the counter orchestration because it will wait indefinitely for input.
            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.Counter),
                input: 0,
                instanceId: singletonInstanceId);

            var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Running, status?.OrchestrationStatus);
            Assert.Equal("0", status?.Input);
            Assert.Null(status?.Output);

            client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.Counter),
                input: 99,
                instanceId: singletonInstanceId);

            // Note that with extended sessions, the startup time may take longer because the dispatcher
            // will wait for the current extended session to expire before the new create message is accepted.
            status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(20));

            Assert.Equal(OrchestrationStatus.Running, status?.OrchestrationStatus);
            Assert.Equal("99", status?.Input);
        }

        /// <summary>
        /// End-to-end test which validates that a running orchestration can be deduplicated.
        /// </summary>
        [Fact]
        public async Task DeduplicateRunningInstance()
        {
            string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

            // Using the counter orchestration because it will wait indefinitely for input.
            var client = await this.host.StartOrchestrationAsync(
                typeof(Orchestrations.Counter),
                input: 0,
                instanceId: singletonInstanceId);

            var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
            var state1 = (await client.GetStateAsync(singletonInstanceId)).First();

            Assert.Equal(OrchestrationStatus.Running, status?.OrchestrationStatus);
            Assert.Equal("0", status?.Input);
            Assert.Null(status?.Output);

            (bool created, _) = await this.host.StartDeduplicatedOrchestrationAsync(
               typeof(Orchestrations.SayHelloWithActivity),
               input: "Two",
               instanceId: singletonInstanceId,
               new OrchestrationStatus[] { OrchestrationStatus.Running });

            Assert.False(created);

            var state2 = (await client.GetStateAsync(singletonInstanceId)).First();
            Assert.Equal(state1.OrchestrationInstance.ExecutionId, state2.OrchestrationInstance.ExecutionId);
        }

        [Fact]
        public async Task TimerCancellation()
        {
            var timeout = TimeSpan.FromSeconds(10);
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
            await client.RaiseEventAsync("approval", eventData: true);

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("Approved", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates the handling of durable timer expiration.
        /// </summary>
        [Fact]
        public async Task TimerExpiration()
        {
            var timeout = TimeSpan.FromSeconds(10);
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            // Don't send any notification - let the internal timeout expire

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("Expired", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates the handling of an orchestration with very large input and output.
        /// </summary>
        [Fact]
        public async Task LargeInputAndOutput()
        {
            var random = new Random();
            var bytes = new byte[5 * 1024 * 1024];
            random.NextBytes(bytes);

            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.EchoBytes), bytes);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(300)); // can take quite long if EH connection is slow or throttled

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal<byte>(bytes, JsonConvert.DeserializeObject<byte[]>(status?.Input));
            Assert.Equal<byte>(bytes, JsonConvert.DeserializeObject<byte[]>(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations run concurrently of each other (up to 100 by default).
        /// </summary>
        [Fact]
        public async Task OrchestrationConcurrency()
        {
            Func<Task> orchestrationStarter = async () =>
            {
                System.Diagnostics.Trace.TraceInformation($"Starting orchestration");
                try
                {
                    var timeout = TimeSpan.FromSeconds(10);
                    var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    System.Diagnostics.Trace.TraceInformation($"Starting wait for {client.InstanceId}");
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    // Don't send any notification - let the internal timeout expire
                }
                catch (Exception e)
                {
                    System.Diagnostics.Trace.TraceInformation($"Error in orchestration: {e}");
                }
                finally
                {
                    System.Diagnostics.Trace.TraceInformation($"Completed orchestration");
                }
            };

            int iterations = 10;
            var tasks = new Task[iterations];
            for (int i = 0; i < iterations; i++)
            {
                tasks[i] = orchestrationStarter();
            }

            // The 10 orchestrations above (which each delay for 10 seconds) should all complete in less than 60 seconds.
            Task parallelOrchestrations = Task.WhenAll(tasks);
            Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(60));

            Task winner = await Task.WhenAny(parallelOrchestrations, timeoutTask);
            foreach (var t in tasks)
            {
                await t;
            }
            Assert.Equal(parallelOrchestrations, winner);
        }

        /// <summary>
        /// End-to-end test which validates the orchestrator's exception handling behavior.
        /// </summary>
        [Fact]
        public async Task HandledActivityException()
        {
            // Empty string input should result in ArgumentNullException in the orchestration code.
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.TryCatchLoop), 5);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(5, JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from orchestrator code.
        /// </summary>
        [Fact]
        public async Task UnhandledOrchestrationException()
        {
            // Empty string input should result in ArgumentNullException in the orchestration code.
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Throw), "");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Failed, status?.OrchestrationStatus);
            Assert.True(status?.Output.Contains("null") == true);
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from activity code.
        /// </summary>
        [Fact]
        public async Task UnhandledActivityException()
        {
            string message = "Kah-BOOOOM!!!";
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Throw), message);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Failed, status?.OrchestrationStatus);
            Assert.True(status?.Output.Contains(message) == true);
        }

        /// <summary>
        /// Fan-out/fan-in test which ensures each operation is run only once.
        /// </summary>
        [Fact]
        public async Task FanOutToTableStorage()
        {
            int iterations = 100;

            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.MapReduceTableStorage), iterations);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(120));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(iterations, int.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with <=60KB text message sizes can run successfully.
        /// </summary>
        [Fact]
        public async Task SmallTextMessagePayloads()
        {
            // Generate a small random string payload
            const int TargetPayloadSize = 1 * 1024; // 1 KB
            const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.-";
            var sb = new StringBuilder();
            var random = new Random();
            while (Encoding.Unicode.GetByteCount(sb.ToString()) < TargetPayloadSize)
            {
                for (int i = 0; i < 1000; i++)
                {
                    sb.Append(Chars[random.Next(Chars.Length)]);
                }
            }

            string message = sb.ToString();
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(message, JToken.Parse(status?.Output));
        }

        StringBuilder GenerateMediumRandomStringPayload()
        {
            // Generate a medium random string payload
            const int TargetPayloadSize = 128 * 1024; // 128 KB
            const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.-";
            var sb = new StringBuilder();
            var random = new Random();
            while (Encoding.Unicode.GetByteCount(sb.ToString()) < TargetPayloadSize)
            {
                for (int i = 0; i < 1000; i++)
                {
                    sb.Append(Chars[random.Next(Chars.Length)]);
                }
            }

            return sb;
        }

        /// <summary>
        /// Tests an orchestration that does two consecutive fan-out, fan-ins.
        /// This is a regression test for https://github.com/Azure/durabletask/issues/241.
        /// </summary>
        [Fact]
        public async Task DoubleFanOut()
        {
            var client = await this.host.StartOrchestrationAsync(typeof(Orchestrations.DoubleFanOut), null);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        }

        /// <summary>
        /// Validates scheduled starts. Runs three tests in parallel because they each wait so long.
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [Fact]
        public async Task ScheduledStart_All()
        {
            // run these three tests in parallel because they each wait so long

            var test1 = this.ScheduledStart_Activity();
            var test2 = this.ScheduledStart_Activity_GetStatus_Returns_ScheduledStart();
            var test3 = this.ScheduledStart_Activity_GetStatus_Returns_ScheduledStart();

            await Task.WhenAll(test1, test2, test3);
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        async Task ScheduledStart_Inline()
        {
            var expectedStartTime = DateTime.UtcNow.AddSeconds(10);
            var clientStartingIn10Seconds = await this.host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), "Current Time!", startAt: expectedStartTime);
            var clientStartingNow = await this.host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), "Current Time!");

            var statusStartingNow = clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            var statusStartingIn10Seconds = clientStartingIn10Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            await Task.WhenAll(statusStartingNow, statusStartingIn10Seconds);

            Assert.Equal(OrchestrationStatus.Completed, statusStartingNow.Result?.OrchestrationStatus);
            Assert.Equal("Current Time!", JToken.Parse(statusStartingNow.Result?.Input));
            Assert.Null(statusStartingNow.Result.ScheduledStartTime);

            Assert.Equal(OrchestrationStatus.Completed, statusStartingIn10Seconds.Result?.OrchestrationStatus);
            Assert.Equal("Current Time!", JToken.Parse(statusStartingIn10Seconds.Result?.Input));
            Assert.Equal(expectedStartTime, statusStartingIn10Seconds.Result.ScheduledStartTime);

            var startNowResult = (DateTime)JToken.Parse(statusStartingNow.Result?.Output);
            var startIn10SecondsResult = (DateTime)JToken.Parse(statusStartingIn10Seconds.Result?.Output);

            Assert.True(startIn10SecondsResult > startNowResult);
            Assert.True(startIn10SecondsResult >= expectedStartTime);
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        async Task ScheduledStart_Activity()
        {
            var expectedStartTime = DateTime.UtcNow.AddSeconds(10);
            var clientStartingIn10Seconds = await this.host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeActivity), "Current Time!", startAt: expectedStartTime);
            var clientStartingNow = await this.host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeActivity), "Current Time!");

            var statusStartingNow = clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            var statusStartingIn10Seconds = clientStartingIn10Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            await Task.WhenAll(statusStartingNow, statusStartingIn10Seconds);

            Assert.Equal(OrchestrationStatus.Completed, statusStartingNow.Result?.OrchestrationStatus);
            Assert.Equal("Current Time!", JToken.Parse(statusStartingNow.Result?.Input));
            Assert.Null(statusStartingNow.Result.ScheduledStartTime);

            Assert.Equal(OrchestrationStatus.Completed, statusStartingIn10Seconds.Result?.OrchestrationStatus);
            Assert.Equal("Current Time!", JToken.Parse(statusStartingIn10Seconds.Result?.Input));
            Assert.Equal(expectedStartTime, statusStartingIn10Seconds.Result.ScheduledStartTime);

            var startNowResult = (DateTime)JToken.Parse(statusStartingNow.Result?.Output);
            var startIn10SecondsResult = (DateTime)JToken.Parse(statusStartingIn10Seconds.Result?.Output);

            Assert.True(startIn10SecondsResult > startNowResult);
            Assert.True(startIn10SecondsResult >= expectedStartTime);
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        async Task ScheduledStart_Activity_GetStatus_Returns_ScheduledStart()
        {
            var expectedStartTime = DateTime.UtcNow.AddSeconds(10);
            var clientStartingIn10Seconds = await this.host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeActivity), "Delayed Current Time!", startAt: expectedStartTime);
            var clientStartingNow = await this.host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeActivity), "Delayed Current Time!");

            var statusStartingIn10Seconds = await clientStartingIn10Seconds.GetStateAsync();
            Assert.NotNull(statusStartingIn10Seconds.ScheduledStartTime);
            Assert.Equal(expectedStartTime, statusStartingIn10Seconds.ScheduledStartTime);

            var statusStartingNow = await clientStartingNow.GetStateAsync();
            Assert.Null(statusStartingNow.ScheduledStartTime);

            await Task.WhenAll(
                clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(35)),
                clientStartingIn10Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(65))
                );
        }


        public static class Orchestrations
        {
            internal class SayHelloInline : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult($"Hello, {input}!");
                }
            }

            [KnownType(typeof(SayHelloInline))]
            internal class SayHelloFanOutFanIn: TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int count)
                {
                    if (count > 10000)
                        throw new ArgumentException("count cannot exceed 10000", nameof(count));

                    var tasks = Enumerable
                        .Range(0, count)
                        .Select(i => context.CreateSubOrchestrationInstance<string>(typeof(SayHelloInline), $"{i:D5}"))
                        .ToList();

                    await Task.WhenAll(tasks);

                    return string.Join(',', tasks.Select(t => t.Result.Substring(7, 5)));
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class SayHelloWithActivity : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.Hello), input);
                }
            }

            [KnownType(typeof(Activities.Hello))]
            public class Hello5 : TaskOrchestration<List<string>, string>
            {
                public override async Task<List<string>> RunTask(OrchestrationContext context, string input)
                {
                    var outputs = new List<string>
                    {
                        await context.ScheduleTask<string>(typeof(Activities.Hello), "Tokyo"),
                        await context.ScheduleTask<string>(typeof(Activities.Hello), "Seattle"),
                        await context.ScheduleTask<string>(typeof(Activities.Hello), "London"),
                        await context.ScheduleTask<string>(typeof(Activities.Hello), "Amsterdam"),
                        await context.ScheduleTask<string>(typeof(Activities.Hello), "Mumbai")
                    };
                    return outputs;
                }
            }

            [KnownType(typeof(Activities.HelloFailActivity))]
            internal class SayHelloWithActivityFail : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.HelloFailActivity), input);
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class Factorial : TaskOrchestration<long, int>
            {
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialFail : TaskOrchestration<long, int>
            {
                public static bool ShouldFail = true;
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    if (ShouldFail)
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialOrchestratorFail : TaskOrchestration<long, int>
            {
                public static bool ShouldFail = true;
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    if (ShouldFail)
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.MultiplyMultipleActivityFail))]
            internal class FactorialMultipleActivityFail : TaskOrchestration<long, int>
            {
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.MultiplyMultipleActivityFail), new[] { result, i }));
                    }

                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialNoReplay : Factorial
            {
                public override Task<long> RunTask(OrchestrationContext context, int n)
                {
                    if (context.IsReplaying)
                    {
                        throw new Exception("Replaying is forbidden in this test.");
                    }

                    return base.RunTask(context, n);
                }
            }

            [KnownType(typeof(Activities.GetFileList))]
            [KnownType(typeof(Activities.GetFileSize))]
            internal class DiskUsage : TaskOrchestration<long, string>
            {
                public override async Task<long> RunTask(OrchestrationContext context, string directory)
                {
                    string[] files = await context.ScheduleTask<string[]>(typeof(Activities.GetFileList), directory);

                    var tasks = new Task<long>[files.Length];
                    for (int i = 0; i < files.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<long>(typeof(Activities.GetFileSize), files[i]);
                    }

                    await Task.WhenAll(tasks);

                    long totalBytes = tasks.Sum(t => t.Result);
                    return totalBytes;
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class FanOutFanIn : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Hello), i.ToString("000"));
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }
            }

            [KnownType(typeof(Activities.HelloFailFanOut))]
            internal class FanOutFanInRewind : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.HelloFailFanOut), i.ToString("000"));
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }
            }

            [KnownType(typeof(Activities.Echo))]
            internal class SemiLargePayloadFanOutFanIn : TaskOrchestration<string, int>
            {
                static readonly string Some50KBPayload = new string('x', 25 * 1024); // Assumes UTF-16 encoding
                static readonly string Some16KBPayload = new string('x', 8 * 1024); // Assumes UTF-16 encoding

                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Echo), Some50KBPayload);
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }

                public override string GetStatus()
                {
                    return Some16KBPayload;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowSubOrchestrationFail))]
            [KnownType(typeof(Activities.Hello))]
            public class ChildWorkflowSubOrchestrationFail : TaskOrchestration<string, int>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating sub-orchestration failure...");
                    }
                    var result = await context.ScheduleTask<string>(typeof(Activities.Hello), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowSubOrchestrationActivityFail))]
            [KnownType(typeof(Activities.HelloFailSubOrchestrationActivity))]
            public class ChildWorkflowSubOrchestrationActivityFail : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    var result = await context.ScheduleTask<string>(typeof(Activities.HelloFailSubOrchestrationActivity), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.GrandparentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ParentWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class ChildWorkflowNestedActivityFail : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    var result = await context.ScheduleTask<string>(typeof(Activities.HelloFailNestedSuborchestration), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.ChildWorkflowSubOrchestrationFail))]
            [KnownType(typeof(Activities.Hello))]
            public class ParentWorkflowSubOrchestrationFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowSubOrchestrationFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }


            [KnownType(typeof(Orchestrations.GrandparentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ChildWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class ParentWorkflowNestedActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowNestedActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            [KnownType(typeof(Orchestrations.ChildWorkflowSubOrchestrationActivityFail))]
            [KnownType(typeof(Activities.HelloFailSubOrchestrationActivity))]
            public class ParentWorkflowSubOrchestrationActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowSubOrchestrationActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ChildWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class GrandparentWorkflowNestedActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ParentWorkflowNestedActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            internal class Counter : TaskOrchestration<int, int>
            {
                TaskCompletionSource<string> waitForOperationHandle;
                internal const string OpEventName = "operation";
                internal const string OpIncrement = "incr";
                internal const string OpDecrement = "decr";
                internal const string OpEnd = "end";

                public override async Task<int> RunTask(OrchestrationContext context, int currentValue)
                {
                    string operation = await this.WaitForOperation();

                    bool done = false;
                    switch (operation?.ToLowerInvariant())
                    {
                        case OpIncrement:
                            currentValue++;
                            break;
                        case OpDecrement:
                            currentValue--;
                            break;
                        case OpEnd:
                            done = true;
                            break;
                    }

                    if (!done)
                    {
                        context.ContinueAsNew(currentValue);
                    }
                    return currentValue;
                }

                async Task<string> WaitForOperation()
                {
                    this.waitForOperationHandle = new TaskCompletionSource<string>();
                    string operation = await this.waitForOperationHandle.Task;
                    this.waitForOperationHandle = null;
                    return operation;
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    Assert.Equal(OpEventName, name);
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }

            internal class CharacterCounter : TaskOrchestration<Tuple<string, int>, Tuple<string, int>>
            {
                TaskCompletionSource<string> waitForOperationHandle;

                public override async Task<Tuple<string, int>> RunTask(OrchestrationContext context, Tuple<string, int> inputData)
                {
                    string operation = await this.WaitForOperation();
                    bool done = false;
                    switch (operation?.ToLowerInvariant())
                    {
                        case "double":
                            inputData = new Tuple<string, int>(
                                $"{inputData.Item1}{inputData.Item1.Reverse()}",
                                inputData.Item2 * 2);
                            break;
                        case "end":
                            done = true;
                            break;
                    }

                    if (!done)
                    {
                        context.ContinueAsNew(inputData);
                    }

                    return inputData;
                }

                async Task<string> WaitForOperation()
                {
                    this.waitForOperationHandle = new TaskCompletionSource<string>();
                    string operation = await this.waitForOperationHandle.Task;
                    this.waitForOperationHandle = null;
                    return operation;
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    Assert.Equal("operation", name);
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }

            internal class Approval : TaskOrchestration<string, TimeSpan, bool, string>
            {
                TaskCompletionSource<bool> waitForApprovalHandle;
                public static bool shouldFail = false;

                public override async Task<string> RunTask(OrchestrationContext context, TimeSpan timeout)
                {
                    DateTime deadline = context.CurrentUtcDateTime.Add(timeout);

                    using (var cts = new CancellationTokenSource())
                    {
                        Task<bool> approvalTask = this.GetWaitForApprovalTask();
                        Task timeoutTask = context.CreateTimer(deadline, cts.Token);

                        if (shouldFail)
                        {
                            throw new Exception("Simulating unhanded error exception");
                        }

                        if (approvalTask == await Task.WhenAny(approvalTask, timeoutTask))
                        {
                            // The timer must be cancelled or fired in order for the orchestration to complete.
                            cts.Cancel();

                            bool approved = approvalTask.Result;
                            return approved ? "Approved" : "Rejected";
                        }
                        else
                        {
                            return "Expired";
                        }
                    }
                }

                async Task<bool> GetWaitForApprovalTask()
                {
                    this.waitForApprovalHandle = new TaskCompletionSource<bool>();
                    bool approvalResult = await this.waitForApprovalHandle.Task;
                    this.waitForApprovalHandle = null;
                    return approvalResult;
                }

                public override void OnEvent(OrchestrationContext context, string name, bool approvalResult)
                {
                    Assert.Equal("approval", name);
                    if (this.waitForApprovalHandle != null)
                    {
                        this.waitForApprovalHandle.SetResult(approvalResult);
                    }
                }
            }

            [KnownType(typeof(Activities.Throw))]
            internal class Throw : TaskOrchestration<string, string>
            {
                public override async Task<string> RunTask(OrchestrationContext context, string message)
                {
                    if (string.IsNullOrEmpty(message))
                    {
                        // This throw happens directly in the orchestration.
                        throw new ArgumentNullException(nameof(message));
                    }

                    // This throw happens in the implementation of an activity.
                    await context.ScheduleTask<string>(typeof(Activities.Throw), message);
                    return null;
                }
            }

            [KnownType(typeof(Activities.Throw))]
            internal class TryCatchLoop : TaskOrchestration<int, int>
            {
                public override async Task<int> RunTask(OrchestrationContext context, int iterations)
                {
                    int catchCount = 0;

                    for (int i = 0; i < iterations; i++)
                    {
                        try
                        {
                            await context.ScheduleTask<string>(typeof(Activities.Throw), "Kah-BOOOOOM!!!");
                        }
                        catch (TaskFailedException)
                        {
                            catchCount++;
                        }
                    }

                    return catchCount;
                }
            }

            [KnownType(typeof(Activities.Echo))]
            internal class Echo : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.Echo), input);
                }
            }

            [KnownType(typeof(Activities.EchoBytes))]
            internal class EchoBytes : TaskOrchestration<byte[], byte[]>
            {
                public override Task<byte[]> RunTask(OrchestrationContext context, byte[] input)
                {
                    return context.ScheduleTask<byte[]>(typeof(Activities.EchoBytes), input);
                }
            }

            [KnownType(typeof(Activities.WriteTableRow))]
            [KnownType(typeof(Activities.CountTableRows))]
            internal class MapReduceTableStorage : TaskOrchestration<int, int>
            {
                public override async Task<int> RunTask(OrchestrationContext context, int iterations)
                {
                    string instanceId = context.OrchestrationInstance.InstanceId;

                    var tasks = new List<Task>(iterations);
                    for (int i = 1; i <= iterations; i++)
                    {
                        tasks.Add(context.ScheduleTask<string>(
                            typeof(Activities.WriteTableRow),
                            new Tuple<string, string>(instanceId, i.ToString("000"))));
                    }

                    await Task.WhenAll(tasks);

                    return await context.ScheduleTask<int>(typeof(Activities.CountTableRows), instanceId);
                }
            }

            [KnownType(typeof(Factorial))]
            [KnownType(typeof(Activities.Multiply))]
            internal class ParentOfFactorial : TaskOrchestration<int, int>
            {
                public override Task<int> RunTask(OrchestrationContext context, int input)
                {
                    return context.CreateSubOrchestrationInstance<int>(typeof(Factorial), input);
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class DoubleFanOut : TaskOrchestration<string, string>
            {
                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    Random r = new Random();
                    var tasks = new Task<string>[5];
                    for (int i = 0; i < 5; i++)
                    {
                        int x = r.Next(10000);
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Hello), i.ToString());
                    }

                    await Task.WhenAll(tasks);

                    var tasks2 = new Task<string>[5];
                    for (int i = 0; i < 5; i++)
                    {
                        int x = r.Next(10000);
                        tasks2[i] = context.ScheduleTask<string>(typeof(Activities.Hello), (i + 10).ToString());
                    }

                    await Task.WhenAll(tasks2);

                    return "OK";
                }
            }

            [KnownType(typeof(AutoStartOrchestration.Responder))]
            internal class AutoStartOrchestration : TaskOrchestration<string, string>
            {
                readonly TaskCompletionSource<string> tcs = new TaskCompletionSource<string>();

                // HACK: This is just a hack to communicate result of orchestration back to test
                public static bool OkResult;

                const string ChannelName = "conversation";

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    var responderId = $"@{typeof(Responder).FullName}";
                    var responderInstance = new OrchestrationInstance() { InstanceId = responderId };

                    // send the id of this orchestration to a not-yet-started orchestration
                    context.SendEvent(responderInstance, ChannelName, context.OrchestrationInstance.InstanceId);

                    // wait for a response event 
                    var message = await this.tcs.Task;
                    if (message != "hello from autostarted orchestration")
                        throw new Exception("test failed");

                    OkResult = true;

                    return "OK";
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    if (name == ChannelName)
                    {
                        this.tcs.TrySetResult(input);
                    }
                }

                public class Responder : TaskOrchestration<string, string>
                {
                    readonly TaskCompletionSource<string> tcs = new TaskCompletionSource<string>();

                    public async override Task<string> RunTask(OrchestrationContext context, string input)
                    {
                        var message = await this.tcs.Task;
                        string responseString;

                        // send a message back to the sender
                        if (input != null)
                        {
                            responseString = "expected null input for autostarted orchestration";
                        }
                        else
                        {
                            responseString = "hello from autostarted orchestration";
                        }
                        var senderInstance = new OrchestrationInstance() { InstanceId = message };
                        context.SendEvent(senderInstance, ChannelName, responseString);

                        context.ContinueAsNew(null);

                        return "this return value is not observed by anyone";
                    }

                    public override void OnEvent(OrchestrationContext context, string name, string input)
                    {
                        if (name == ChannelName)
                        {
                            this.tcs.TrySetResult(input);
                        }
                    }
                }
            }

            internal class CurrentTimeInline : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult(context.CurrentUtcDateTime);
                }
            }

            [KnownType(typeof(Activities.CurrentTime))]
            internal class CurrentTimeActivity : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<DateTime>(typeof(Activities.CurrentTime), input);
                }
            }

            internal class DelayedCurrentTimeInline : TaskOrchestration<DateTime, string>
            {
                public override async Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    await context.CreateTimer<bool>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(3)), true);
                    return context.CurrentUtcDateTime;
                }
            }

            [KnownType(typeof(Activities.DelayedCurrentTime))]
            internal class DelayedCurrentTimeActivity : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<DateTime>(typeof(Activities.DelayedCurrentTime), input);
                }
            }
        }

        public static class Activities
        {
            internal class HelloFailActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailFanOut : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2) //&& (input == "0" || input == "2"))
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailMultipleActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailNestedSuborchestration : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailSubOrchestrationActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            public class Hello : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }
                    return $"Hello, {input}!";
                }
            }

            internal class Multiply : TaskActivity<long[], long>
            {
                protected override long Execute(TaskContext context, long[] values)
                {
                    return values[0] * values[1];
                }
            }

            internal class MultiplyMultipleActivityFail : TaskActivity<long[], long>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override long Execute(TaskContext context, long[] values)
                {
                    if ((ShouldFail1 && (values[1] == 1)) || (ShouldFail2 && values[1] == 2))
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }

                    return values[0] * values[1];
                }
            }
            internal class MultiplyFailOrchestration : TaskActivity<long[], long>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override long Execute(TaskContext context, long[] values)
                {
                    if ((ShouldFail1 && (values[1] == 1)) || (ShouldFail2 && values[1] == 2))
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }

                    return values[0] * values[1];
                }
            }


            internal class GetFileList : TaskActivity<string, string[]>
            {
                protected override string[] Execute(TaskContext context, string directory)
                {
                    return Directory.GetFiles(directory, "*", SearchOption.TopDirectoryOnly);
                }
            }

            internal class GetFileSize : TaskActivity<string, long>
            {
                protected override long Execute(TaskContext context, string fileName)
                {
                    var info = new FileInfo(fileName);
                    return info.Length;
                }
            }

            internal class Throw : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string message)
                {
                    throw new Exception(message);
                }
            }

            internal class WriteTableRow : TaskActivity<Tuple<string, string>, string>
            {
                static TableClient tableClient;

                internal static TableClient TestCloudTable
                {
                    get
                    {
                        if (tableClient == null)
                        {
                            TableClient table = new TableClient(Environment.GetEnvironmentVariable(TestConstants.StorageConnectionName), "TestTable");
                            table.CreateIfNotExistsAsync().Wait();
                            tableClient = table;
                        }

                        return tableClient;
                    }
                }

                protected override string Execute(TaskContext context, Tuple<string, string> rowData)
                {
                    var entity = new TableEntity(rowData.Item1, $"{rowData.Item2}.{Guid.NewGuid():N}");
                    WriteTableRow.TestCloudTable.AddEntity(entity);
                    return null;
                }
            }

            

            internal class CountTableRows : TaskActivity<string, int>
            {
                protected override int Execute(TaskContext context, string partitionKey)
                {
                    var asyncPageable =  WriteTableRow.TestCloudTable.QueryAsync<TableEntity>($"PartitionKey eq '{partitionKey}'");
                    var count = asyncPageable.CountAsync().GetAwaiter().GetResult();
                    return count;
                }
            }

            internal class Echo : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string input)
                {
                    return input;
                }
            }

            internal class EchoBytes : TaskActivity<byte[], byte[]>
            {
                protected override byte[] Execute(TaskContext context, byte[] input)
                {
                    return input;
                }
            }

            internal class CurrentTime : TaskActivity<string, DateTime>
            {
                protected override DateTime Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }
                    return DateTime.UtcNow;
                }
            }

            internal class DelayedCurrentTime : TaskActivity<string, DateTime>
            {
                protected override DateTime Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    return DateTime.UtcNow;
                }
            }
        }
    }
}
