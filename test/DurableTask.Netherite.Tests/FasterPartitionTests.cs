// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "false")]
    public class FasterPartitionTests : IDisposable
    {
        readonly SingleHostFixture.TestTraceListener traceListener;
        readonly ILoggerFactory loggerFactory;
        readonly XunitLoggerProvider provider;
        readonly Action<string> output;
        ITestOutputHelper outputHelper;

        public FasterPartitionTests(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            this.output = (string message) => this.outputHelper?.WriteLine(message);

            this.loggerFactory = new LoggerFactory();
            this.provider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(this.provider);
            this.traceListener = new SingleHostFixture.TestTraceListener();
            Trace.Listeners.Add(this.traceListener);
            this.traceListener.Output = this.output;
        }

        public void Dispose()
        {
            this.outputHelper = null;
            Trace.Listeners.Remove(this.traceListener);
        }

        /// <summary>
        /// Create a partition and then restore it.
        /// </summary>
        [Fact]
        public async Task CreateThenRestore()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var worker = new TaskHubWorker(service);
                var client = new TaskHubClient(service);
                worker.AddTaskOrchestrations(orchestrationType);
                await worker.StartAsync();

                // do orchestration
                var instance = await client.CreateOrchestrationInstanceAsync(orchestrationType, "0", "0");
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(20));

                // stop the service
                await orchestrationService.StopAsync();
            }
            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var client = new TaskHubClient(orchestrationServiceClient);

                var orchestrationState = await client.GetOrchestrationStateAsync("0");
                Assert.Equal(OrchestrationStatus.Completed, orchestrationState.OrchestrationStatus);

                // stop the service
                await orchestrationService.StopAsync();
            }
        }

        /// <summary>
        /// Run a number of orchestrations that requires more memory than available for FASTER
        /// </summary>
        [Fact(Skip ="CachedDebugger will only work for Faster v2")]
        public async Task LimitedMemory()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.PartitionCount = 1;
            settings.ResolvedTransportConnectionString = "MemoryF"; // don't bother with EventHubs for this test

            // use a fresh hubname on every run
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";

            // we don't want to take checkpoints in this test, so we set the following checkpoint triggers unattainably high
            settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 * 1024 * 1024;
            settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
            settings.IdleCheckpointFrequencyMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;

            // set the memory size very small so we can force evictions
            settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 10,       // 1 KB
                StoreLogMemorySizeBits = 12,     // 4 KB, which means only about 166 entries fit into memory
            };

            // create a cache monitor
            var cacheDebugger = new Faster.CacheDebugger(settings.TestHooks);
            var cts = new CancellationTokenSource();
            string reportedProblem = null;
            settings.TestHooks.OnError += (message) =>
            {
                this.output?.Invoke($"TESTHOOKS: {message}");
                reportedProblem = reportedProblem ?? message;
                cts.Cancel();
            };
            settings.TestHooks.CacheDebugger = cacheDebugger;

            // we use the standard hello orchestration from the samples, which calls 5 activities in sequence
            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 100; // requires 200 FASTER key-value pairs so it does not fit into memory

            // start the service 
            var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
            var orchestrationService = (IOrchestrationService)service;
            await orchestrationService.CreateAsync();
            await orchestrationService.StartAsync();
            var host = (TransportAbstraction.IHost)service;
            Assert.Equal(1u, service.NumberPartitions);
            var worker = new TaskHubWorker(service);
            var client = new TaskHubClient(service);
            worker.AddTaskOrchestrations(orchestrationType);
            worker.AddTaskActivities(activityType);
            await worker.StartAsync();

            // start all orchestrations
            {
                var tasks = new Task[OrchestrationCount];
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, InstanceId(i), null);

                var timeout = TimeSpan.FromMinutes(3);
                var terminationTask = Task.Delay(timeout, cts.Token);
                var completionTask = Task.WhenAll(tasks);
                var firstTask = await Task.WhenAny(terminationTask, completionTask);
                Assert.True(reportedProblem == null, $"CacheDebugger detected problem while starting orchestrations: {reportedProblem}");
                Assert.True(firstTask != terminationTask, $"timed out after {timeout} while starting orchestrations");
            }

            // wait for all orchestrations to finish executing
            this.output?.Invoke("waiting for orchestrations to finish executing...");
            try
            {
                async Task WaitFor(int i)
                {
                    try
                    {
                        await client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(10));
                    }
                    catch (Exception e)
                    {
                        this.output?.Invoke($"Orchestration {InstanceId(i)} failed with {e.GetType()}: {e.Message}");
                    }
                }

                var tasks = new Task[OrchestrationCount];
                var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromMinutes(3);
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = WaitFor(i);

                void PrintUnfinished()
                {
                    var sb = new StringBuilder();
                    sb.Append("Waiting for orchestrations:");
                    for (int i = 0; i < OrchestrationCount; i++)
                    {
                        if (!tasks[i].IsCompleted)
                        {
                            sb.Append(' ');
                            sb.Append(InstanceId(i));
                        }
                    }
                    this.output?.Invoke(sb.ToString());
                }

                void ProgressReportThread()
                {
                    Stopwatch elapsed = new Stopwatch();
                    elapsed.Start();

                    while (elapsed.Elapsed < timeout)
                    {
                        Thread.Sleep(10000);
                        PrintUnfinished();
                    }

                    cts.Cancel();
                }
                var thread = new Thread(ProgressReportThread);
                thread.Name = "ProgressReportThread";
                thread.Start();

                var terminationTask = Task.Delay(timeout, cts.Token);
                var completionTask = Task.WhenAll(tasks);
                var firstTask = await Task.WhenAny(terminationTask, completionTask);
                Assert.True(reportedProblem == null, $"CacheDebugger detected problem while executing orchestrations: {reportedProblem}");

                PrintUnfinished();

                Assert.True(firstTask != terminationTask, $"timed out after {timeout} while executing orchestrations");

                foreach (var line in cacheDebugger.Dump())
                {
                    this.output?.Invoke(line);
                }
            }
            catch (Exception e)
            {
                this.output?.Invoke($"exception thrown while executing orchestrations: {e}");
                foreach (var line in cacheDebugger.Dump())
                {
                    this.output?.Invoke(line);
                }
                throw;
            }

            // shut down the service
            await orchestrationService.StopAsync();
        }



        /// <summary>
        /// Test behavior of queries and point queries
        /// </summary>
        [Fact]
        public async Task QueriesCopyToTail()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";
            var checkpointInjector = settings.TestHooks.CheckpointInjector = new Faster.CheckpointInjector(settings.TestHooks);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloFanOutFanIn);
            var orchestrationType2 = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceClient)service;
                var orchestrationServiceQueryClient = (IOrchestrationServiceQueryClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var worker = new TaskHubWorker(service);
                var client = new TaskHubClient(service);
                worker.AddTaskOrchestrations(orchestrationType);
                worker.AddTaskOrchestrations(orchestrationType2);
                await worker.StartAsync();

                // create 100 instances
                var instance = await client.CreateOrchestrationInstanceAsync(orchestrationType, "parent", 99);
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(40));
                var instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Equal(100, instances.Count);

                // check that log contains 200 records
                var log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(200 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.BeginAddress);

                // take a foldover checkpoint
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                Assert.Equal(200 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // read all instances using a query and check that the log did not grow
                // (because queries do not copy to tail)
                instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Equal(100, instances.Count);
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                Assert.Equal(200 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // read all instances using point queries and check that the log grew by one record per instance
                // (because point queries read the InstanceState on the main session, which copies it to the tail)
                var tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                await Task.WhenAll(tasks);
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(300 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress - 100 * log.FixedRecordSize);

                // doing the same again has no effect
                // (because all instances are already in the mutable section)
                tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                await Task.WhenAll(tasks);
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(300 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress - 100 * log.FixedRecordSize);

                // take a foldover checkpoint
                // this moves the readonly section back to the end
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                Assert.Equal(300 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // stop the service
                await orchestrationService.StopAsync();
            }
        }

        /// <summary>
        /// Test log compaction
        /// </summary>
        [Fact]
        public async Task Compaction()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";
            var checkpointInjector = settings.TestHooks.CheckpointInjector = new Faster.CheckpointInjector(settings.TestHooks);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloFanOutFanIn);
            var orchestrationType2 = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            long compactUntil = 0;

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceClient)service;
                var orchestrationServiceQueryClient = (IOrchestrationServiceQueryClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var worker = new TaskHubWorker(service);
                var client = new TaskHubClient(service);
                worker.AddTaskOrchestrations(orchestrationType);
                worker.AddTaskOrchestrations(orchestrationType2);
                await worker.StartAsync();

                // create 100 instances
                var instance = await client.CreateOrchestrationInstanceAsync(orchestrationType, "parent", 99);
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(40));
                var instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Equal(100, instances.Count);

                // repeat foldover and copy to tail to inflate the log
                for (int i = 0; i < 4; i++)
                {
                    // take a foldover checkpoint
                    var log2 = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                    Assert.Equal((200 + (100 * i)) * log2.FixedRecordSize, log2.TailAddress - log2.BeginAddress);
                    Assert.Equal(log2.ReadOnlyAddress, log2.TailAddress);

                    // read all instances using point queries to force copy to tail
                    var tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                    await Task.WhenAll(tasks);
                }

                // do log compaction
                var log = await checkpointInjector.InjectAsync(log =>
                {
                    compactUntil = 500 * log.FixedRecordSize + log.BeginAddress;
                    Assert.Equal(compactUntil, log.SafeReadOnlyAddress);
                    return (Faster.StoreWorker.CheckpointTrigger.Compaction, compactUntil);
                });

                // check that the compaction had the desired effect
                Assert.Equal(200 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(compactUntil, log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // stop the service
                await orchestrationService.StopAsync();
            }
            {
                // recover the service
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceQueryClient = (IOrchestrationServiceQueryClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);

                // check the log positions
                var log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(200 * log.FixedRecordSize, log.TailAddress - log.BeginAddress);
                Assert.Equal(compactUntil, log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // check the instance count
                var instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Equal(100, instances.Count);
              
                // stop the service
                await orchestrationService.StopAsync();
            }
        }
    }
}