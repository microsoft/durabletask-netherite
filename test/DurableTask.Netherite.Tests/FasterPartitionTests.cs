// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Faster;
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
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly CancellationTokenSource cts;
        readonly CacheDebugger cacheDebugger;

        ITestOutputHelper outputHelper;
        string errorInTestHooks;

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
            this.settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            string timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff");
            this.settings.HubName = $"FasterPartitionTest-{timestamp}";
            this.settings.ResolvedTransportConnectionString = "MemoryF";
            this.cts = new CancellationTokenSource();
            this.cacheDebugger = this.settings.TestHooks.CacheDebugger = new Faster.CacheDebugger(this.settings.TestHooks);
            this.settings.TestHooks.OnError += (message) =>
            {
                this.output($"TESTHOOKS: {message}");
                this.errorInTestHooks = this.errorInTestHooks ?? message;
                this.cts.Cancel();
            };
        }

        public void Dispose()
        {
            this.outputHelper = null;
            Trace.Listeners.Remove(this.traceListener);
        }

        enum CheckpointFrequency
        {
            None,
            Default,
            Frequent
        }

        void SetCheckpointFrequency(CheckpointFrequency frequency)
        {
            switch (frequency)
            {
                case CheckpointFrequency.None:

                    this.settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 * 1024 * 1024;
                    this.settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
                    this.settings.IdleCheckpointFrequencyMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;
                    return;

                case CheckpointFrequency.Frequent:
                    this.settings.MaxNumberEventsBetweenCheckpoints = 1;
                    return;

                default:
                    return;
            }
        }

        async Task<(IOrchestrationService orchestrationService, TaskHubClient client)> StartService(bool recover, Type orchestrationType, Type activityType = null)
        {
            var service = new NetheriteOrchestrationService(this.settings, this.loggerFactory);
            var orchestrationService = (IOrchestrationService)service;
            var orchestrationServiceClient = (IOrchestrationServiceClient)service;
            await orchestrationService.CreateAsync();
            await orchestrationService.StartAsync();
            var host = (TransportAbstraction.IHost)service;
            Assert.Equal(this.settings.PartitionCount, (int)service.NumberPartitions);
            var worker = new TaskHubWorker(service);
            var client = new TaskHubClient(service);
            worker.AddTaskOrchestrations(orchestrationType);
            if (activityType != null)
            {
                worker.AddTaskActivities(activityType);
            }
            await worker.StartAsync();
            return (service, client);
        }

        /// <summary>
        /// Create a partition and then restore it.
        /// </summary>
        [Fact]
        public async Task CreateThenRestore()
        {
            this.settings.PartitionCount = 1;
            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloInline);
            {
                // start the service 
                var (service, client) = await this.StartService(recover: false, orchestrationType);

                // do orchestration
                var instance = await client.CreateOrchestrationInstanceAsync(orchestrationType, "0", "0");
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(20));

                // stop the service
                await service.StopAsync();
            }
            {
                // start the service 
                var (service, client) = await this.StartService(recover: true, orchestrationType);
                var orchestrationState = await client.GetOrchestrationStateAsync("0");
                Assert.Equal(OrchestrationStatus.Completed, orchestrationState?.OrchestrationStatus);

                // stop the service
                await service.StopAsync();
            }
        }

        /// <summary>
        /// Run a number of orchestrations that requires more memory than available for FASTER
        /// </summary>
        [Fact()]
        public async Task LimitedMemory()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            // set the memory size very small so we can force evictions
            this.settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 10,       // 1 KB
                StoreLogMemorySizeBits = 12,     // 4 KB, which means only about 166 entries fit into memory
            };

            // we use the standard hello orchestration from the samples, which calls 5 activities in sequence
            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 100; // requires 200 FASTER key-value pairs so it does not fit into memory

            // start the service 
            var (service, client) = await this.StartService(recover: false, orchestrationType, activityType);

            // start all orchestrations
            {
                var tasks = new Task[OrchestrationCount];
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, InstanceId(i), null);

                var timeout = TimeSpan.FromMinutes(3);
                var terminationTask = Task.Delay(timeout, this.cts.Token);
                var completionTask = Task.WhenAll(tasks);
                var firstTask = await Task.WhenAny(terminationTask, completionTask);
                Assert.True(this.errorInTestHooks == null, $"while starting orchestrations: {this.errorInTestHooks}");
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

                    this.cts.Cancel();
                }
                var thread = new Thread(ProgressReportThread);
                thread.Name = "ProgressReportThread";
                thread.Start();

                var terminationTask = Task.Delay(timeout, this.cts.Token);
                var completionTask = Task.WhenAll(tasks);
                var firstTask = await Task.WhenAny(terminationTask, completionTask);
                Assert.True(this.errorInTestHooks == null, $"while executing orchestrations: {this.errorInTestHooks}");

                PrintUnfinished();

                Assert.True(firstTask != terminationTask, $"timed out after {timeout} while executing orchestrations");

                foreach (var line in this.cacheDebugger.Dump())
                {
                    this.output?.Invoke(line);
                }
            }
            catch (Exception e)
            {
                this.output?.Invoke($"exception thrown while executing orchestrations: {e}");
                foreach (var line in this.cacheDebugger.Dump())
                {
                    this.output?.Invoke(line);
                }
                throw;
            }

            // shut down the service
            await service.StopAsync();
        }

        /// <summary>
        /// Create a partition and then restore it, and use the size tracker again.
        /// </summary>
        [Fact]
        public async Task CheckSizeTrackerOnRecovery()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            // set the memory size very small so we can force evictions
            this.settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 10,       // 1 KB
                StoreLogMemorySizeBits = 12,     // 4 KB, which means only about 166 entries fit into memory
            };

            // we use the standard hello orchestration from the samples, which calls 5 activities in sequence
            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 100; // requires 200 FASTER key-value pairs so it does not fit into memory

            {
                // start the service 
                var (service, client) = await this.StartService(recover: false, orchestrationType, activityType);

                // start all orchestrations
                {
                    var tasks = new Task[OrchestrationCount];
                    for (int i = 0; i < OrchestrationCount; i++)
                        tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, InstanceId(i), null);

                    await Task.WhenAll(tasks);
                    Assert.True(this.errorInTestHooks == null, $"CacheDebugger detected problem while starting orchestrations: {this.errorInTestHooks}");
                }

                // wait for all orchestrations to finish executing
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
                    for (int i = 0; i < OrchestrationCount; i++)
                        tasks[i] = WaitFor(i);
                    await Task.WhenAll(tasks);
                    Assert.True(this.errorInTestHooks == null, $"CacheDebugger detected problem while executing orchestrations: {this.errorInTestHooks}");
                }

                this.output?.Invoke("--- test progress: BEFORE SHUTDOWN ------------------------------------");
                foreach (var line in this.cacheDebugger.Dump())
                {
                    this.output?.Invoke(line);
                }

                // shut down the service
                await service.StopAsync();
            }

            {
                this.output?.Invoke("--- test progress: BEFORE RECOVERY ------------------------------------");

                // recover the service 
                var (service, client) = await this.StartService(recover: true, orchestrationType, activityType);

                this.output?.Invoke("--- test progress: AFTER RECOVERY ------------------------------------");


                // query the status of all orchestrations
                {
                    var tasks = new Task[OrchestrationCount];
                    for (int i = 0; i < OrchestrationCount; i++)
                        tasks[i] = client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(10));
                    await Task.WhenAll(tasks);
                    Assert.True(this.errorInTestHooks == null, $"CacheDebugger detected problem while querying orchestration states: {this.errorInTestHooks}");
                }

                this.output?.Invoke("--- test progress: AFTER QUERIES ------------------------------------");
                foreach (var line in this.cacheDebugger.Dump())
                {
                    this.output?.Invoke(line);
                }

                // shut down the service
                await service.StopAsync();
            }
        }

        /// <summary>
        /// Fill memory, then compute size, then reduce page count, and measure size again
        /// </summary>
        [Fact]
        public async Task CheckMemorySize()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            // we use the standard hello orchestration from the samples, which calls 5 activities in sequence
            var orchestrationType = typeof(ScenarioTests.Orchestrations.SemiLargePayloadFanOutFanIn);
            var activityType = typeof(ScenarioTests.Activities.Echo);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 30;
            int FanOut = 7;

            // start the service 
            var (service, client) = await this.StartService(recover: false, orchestrationType, activityType);

            // run all orchestrations
            {
                var tasks = new Task[OrchestrationCount];
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, InstanceId(i), FanOut);
                await Task.WhenAll(tasks);
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(3));
                await Task.WhenAll(tasks);
                Assert.True(this.errorInTestHooks == null, $"CacheDebugger detected problem while starting orchestrations: {this.errorInTestHooks}");
            }

            (int numPages, long memorySize) = this.cacheDebugger.MemoryTracker.GetMemorySize();

            long historyAndStatusSize = OrchestrationCount * (FanOut * 50000 /* in history */ + 16000 /* in status */);
            Assert.InRange(memorySize, historyAndStatusSize, 1.05 * historyAndStatusSize);
            await service.StopAsync();
        }


        /// <summary>
        /// Fill memory, then compute size, then reduce page count, and measure size again
        /// </summary>
        [Fact]
        public async Task CheckMemoryReduction()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            // set the memory size very small so we can force evictions
            this.settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 9,       // 512 B
                StoreLogMemorySizeBits = 9 + 2, // 16 pages
            };

            // we use the standard hello orchestration from the samples, which calls 5 activities in sequence
            var orchestrationType = typeof(ScenarioTests.Orchestrations.SemiLargePayloadFanOutFanIn);
            var activityType = typeof(ScenarioTests.Activities.Echo);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 50;
            int FanOut = 3;

            // start the service 
            var (service, client) = await this.StartService(recover: false, orchestrationType, activityType);

            // run all orchestrations
            {
                var tasks = new Task[OrchestrationCount];
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, InstanceId(i), FanOut);
                await Task.WhenAll(tasks);
                for (int i = 0; i < OrchestrationCount; i++)
                    tasks[i] = client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(3));
                await Task.WhenAll(tasks);
                Assert.True(this.errorInTestHooks == null, $"CacheDebugger detected problem while starting orchestrations: {this.errorInTestHooks}");
            }

            (int numPages, long memorySize) = this.cacheDebugger.MemoryTracker.GetMemorySize();
            
            long historyAndStatusSize = OrchestrationCount * (FanOut * 50000 /* in history */ + 16000 /* in status */);

            Assert.True(numPages <= 4);
            Assert.True(memorySize < historyAndStatusSize);

            this.cacheDebugger.MemoryTracker.DecrementPages();

            (int numPages2, long memorySize2) = this.cacheDebugger.MemoryTracker.GetMemorySize();

            historyAndStatusSize = OrchestrationCount * (FanOut * 50000 /* in history */ + 16000 /* in status */);

            Assert.True(numPages2 == numPages - 1);

            await service.StopAsync();
        }
    }
}