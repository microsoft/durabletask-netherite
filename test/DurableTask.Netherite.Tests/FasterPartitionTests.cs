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

        const int extraLogEntrySize = 96; // since v2 Faster puts extra stuff in the log. Empirically determined.

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
            Frequent,
            Crazy
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
                    this.settings.MaxNumberEventsBetweenCheckpoints = 100;
                    return;

                case CheckpointFrequency.Crazy:
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
                var thread = TrackedThreads.MakeTrackedThread(ProgressReportThread, "ProgressReportThread");
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
                    Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while starting orchestrations: {this.errorInTestHooks}");
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
                    Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while executing orchestrations: {this.errorInTestHooks}");
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
                    Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while querying orchestration states: {this.errorInTestHooks}");
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
        public async Task PipelinedStart()
        {
            this.settings.PartitionCount = 1;
            this.settings.InstanceCacheSizeMB = 2;
            this.SetCheckpointFrequency(CheckpointFrequency.Frequent);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int numOrchestrations = 500;

            // start the service 
            var (service, client) = await this.StartService(recover: false, orchestrationType, activityType);

            // start all orchestrations and then get the status of each one
            {
                var orchestrations = await Enumerable.Range(0, numOrchestrations).ParallelForEachAsync(200, true, (iteration) =>
                {
                    var orchestrationInstanceId = InstanceId(iteration);
                    return client.CreateOrchestrationInstanceAsync(orchestrationType, orchestrationInstanceId, null);
                });
                Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while starting orchestrations: {this.errorInTestHooks}");
                await Enumerable.Range(0, numOrchestrations).ParallelForEachAsync(200, true, (iteration) =>
                {
                    return client.GetOrchestrationStateAsync(orchestrations[iteration]);
                });  
                Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while checking progress of orchestrations: {this.errorInTestHooks}");
            }

            await service.StopAsync();
        }

        /// <summary>
        /// Repro fail on basic 1000 * hello
        /// </summary>
        [Fact]
        public async Task CheckMemorySize()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SemiLargePayloadFanOutFanIn);
            var activityType = typeof(ScenarioTests.Activities.Echo);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 30;
            int FanOut = 7;
            long historyAndStatusSize = OrchestrationCount * (FanOut * 50000 /* in history */ + 2 * 16000 /* in status */);

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
                Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while starting orchestrations: {this.errorInTestHooks}");
            }

            (int numPages, long memorySize) = this.cacheDebugger.MemoryTracker.GetMemorySize();

            Assert.InRange(memorySize, historyAndStatusSize, 1.05 * historyAndStatusSize);
            await service.StopAsync();
        }


        /// <summary>
        /// Fill up memory, then compute size, then reduce page count, and measure size again
        /// </summary>
        [Fact]
        public async Task CheckMemoryReduction()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            int pageCountBits = 3;
            int pageCount = 1 << pageCountBits;

            // set the memory size very small so we can force evictions
            this.settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 9,       // 512 B
                StoreLogMemorySizeBits = 9 + pageCountBits,  
            };

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SemiLargePayloadFanOutFanIn);
            var activityType = typeof(ScenarioTests.Activities.Echo);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 50;
            int FanOut = 3;
            long historyAndStatusSize = OrchestrationCount * (FanOut * 50000 /* in history */ + 2*16000 /* in status */);

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
                Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while starting orchestrations: {this.errorInTestHooks}");
            }


            {
                (int numPages, long memorySize) = this.cacheDebugger.MemoryTracker.GetMemorySize();
                Assert.InRange(numPages, 1, pageCount);
                Assert.InRange(memorySize, 0, historyAndStatusSize * 1.1);
            }

            int emptyPageCount = 0;
            int tolerance = 1; 

            for (int i = 0; i < 4; i++)
            {
                emptyPageCount++;
                this.cacheDebugger.MemoryTracker.SetEmptyPageCount(emptyPageCount);
                await Task.Delay(TimeSpan.FromSeconds(20));
                (int numPages, long memorySize) = this.cacheDebugger.MemoryTracker.GetMemorySize();
                Assert.InRange(numPages, 1, pageCount - emptyPageCount + tolerance);
                Assert.InRange(memorySize, 0, historyAndStatusSize * 1.1);
            }

            await service.StopAsync();
        }

        /// <summary>
        /// Run orchestrations to exceed the cache size and check that things are evicted to stay within the target
        /// </summary>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CheckMemoryControl(bool useSpaceConsumingOrchestrations)
        {
            this.settings.PartitionCount = 1;
            this.settings.FasterTuningParameters = new BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 10
            };
            this.SetCheckpointFrequency(CheckpointFrequency.None);
            this.settings.TestHooks.CacheDebugger.EnableSizeChecking = false; // our size checker is not entirely accurate in low-memory situationss

            Type orchestrationType, activityType;
            long SizePerInstance;
            object input;
            int portionSize;
            double uppertolerance;
            double lowertolerance;

            if (useSpaceConsumingOrchestrations)
            {
                this.settings.InstanceCacheSizeMB = 4;
                orchestrationType = typeof(ScenarioTests.Orchestrations.SemiLargePayloadFanOutFanIn);
                activityType = typeof(ScenarioTests.Activities.Echo);
                int FanOut = 1;
                input = FanOut;
                SizePerInstance = FanOut * 50000 /* in history */ + 16000 /* in status */;
                portionSize = 50;
                uppertolerance = 1.1;
                lowertolerance = 0;

            }
            else
            {
                this.settings.InstanceCacheSizeMB = 2;
                orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
                activityType = typeof(ScenarioTests.Activities.Hello);
                SizePerInstance = 3610 /* empiric */;
                input = null;
                portionSize = 300;
                uppertolerance = 1.1;
                lowertolerance = 0.8;
            }

            // start the service 
            var (service, client) = await this.StartService(recover: false, orchestrationType, activityType);

            int logBytesPerInstance = 2 * 40;
            long memoryPerPage = ((1 << this.settings.FasterTuningParameters.StoreLogPageSizeBits.Value) / logBytesPerInstance) * SizePerInstance;
            double memoryRangeTo = (this.settings.InstanceCacheSizeMB.Value - 1) * 1024 * 1024;
            double memoryRangeFrom = (memoryRangeTo - memoryPerPage);
            memoryRangeTo = Math.Max(memoryRangeTo, MemoryTracker.MinimumMemoryPages * memoryPerPage);
            memoryRangeTo = uppertolerance * memoryRangeTo;
            memoryRangeFrom = lowertolerance * memoryRangeFrom;
            double pageRangeFrom = Math.Max(MemoryTracker.MinimumMemoryPages, Math.Floor(memoryRangeFrom / memoryPerPage));
            double pageRangeTo = Math.Ceiling(memoryRangeTo / memoryPerPage);

            async Task AddOrchestrationsAsync(int numOrchestrations)
            { 
                var tasks = new Task<OrchestrationInstance>[numOrchestrations];
                for (int i = 0; i < numOrchestrations; i++)
                    tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, Guid.NewGuid().ToString(), input);
                await Task.WhenAll(tasks);
                Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while starting orchestrations: {this.errorInTestHooks}");
                var tasks2 = new Task<OrchestrationState>[numOrchestrations];
                for (int i = 0; i < numOrchestrations; i++)
                    tasks2[i] = client.WaitForOrchestrationAsync(tasks[i].Result, TimeSpan.FromMinutes(3));
                await Task.WhenAll(tasks2);
                Assert.True(this.errorInTestHooks == null, $"TestHooks detected problem while waiting for orchestrations: {this.errorInTestHooks}");
            }

            for (int i = 0; i < 4; i++)
            {
                this.output("memory control ------------------- Add orchestrations");
                {
                    await AddOrchestrationsAsync(portionSize);

                    this.output("memory control -------- wait for effect");
                    await Task.Delay(TimeSpan.FromSeconds(10));

                    this.output("memory control -------- check memory size");
                    (int numPages, long memorySize) = this.cacheDebugger.MemoryTracker.GetMemorySize();

                    Assert.InRange(numPages, pageRangeFrom, pageRangeTo);
                    Assert.InRange(memorySize, memoryRangeFrom, memoryRangeTo);
                }
            }       

            await service.StopAsync();
        }

        /// <summary>
        /// Test behavior of queries and point queries
        /// </summary>
        [Fact]
        public async Task QueriesCopyToTail()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            var checkpointInjector = this.settings.TestHooks.CheckpointInjector = new Faster.CheckpointInjector(this.settings.TestHooks);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloFanOutFanIn);
            var orchestrationType2 = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            {
                // start the service 
                var service = new NetheriteOrchestrationService(this.settings, this.loggerFactory);
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

                int numExtraEntries = 0;

                // check that log contains no records
                var log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(0 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);

                // create 100 instances
                var instance = await client.CreateOrchestrationInstanceAsync(orchestrationType, "parent", 99);
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(40));
                var instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                numExtraEntries += 2;
                Assert.Equal(100, instances.Count);

                // check that log contains 200 records
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(200 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.BeginAddress);

                // take a foldover checkpoint
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                Assert.Equal(200 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // read all instances using a query and check that the log did not grow
                // (because queries do not copy to tail)
                instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Equal(100, instances.Count);
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(200 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // read all instances using point queries and check that the log grew by one record per instance
                // (because point queries read the InstanceState on the main session, which copies it to the tail)
                var tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                await Task.WhenAll(tasks);
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                numExtraEntries += 1;
                Assert.Equal(300 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress - 100 * log.FixedRecordSize - 1 * extraLogEntrySize);

                // doing the same again has no effect
                // (because all instances are already in the mutable section)
                tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                await Task.WhenAll(tasks);
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(300 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress - 100 * log.FixedRecordSize - 1 * extraLogEntrySize);

                // take a foldover checkpoint
                // this moves the readonly section back to the end
                log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                Assert.Equal(300 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
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
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);
            
            var checkpointInjector = this.settings.TestHooks.CheckpointInjector = new Faster.CheckpointInjector(this.settings.TestHooks);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloFanOutFanIn);
            var orchestrationType2 = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            long compactUntil = 0;
            {
                // start the service 
                var service = new NetheriteOrchestrationService(this.settings, this.loggerFactory);
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

                int numExtraEntries = 1;

                // repeat foldover and copy to tail to inflate the log
                for (int i = 0; i < 4; i++)
                {
                    // take a foldover checkpoint
                    var log2 = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                    numExtraEntries += 1;
                    Assert.Equal((200 + (100 * i)) * log2.FixedRecordSize + numExtraEntries * extraLogEntrySize, log2.TailAddress - log2.BeginAddress);
                    Assert.Equal(log2.ReadOnlyAddress, log2.TailAddress);

                    // read all instances using point queries to force copy to tail
                    var tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                    await Task.WhenAll(tasks);
                }

                // do log compaction
                var log = await checkpointInjector.InjectAsync(log =>
                {
                    compactUntil = 500 * log.FixedRecordSize + log.BeginAddress + numExtraEntries * extraLogEntrySize;
                    Assert.Equal(compactUntil, log.SafeReadOnlyAddress);
                    return (Faster.StoreWorker.CheckpointTrigger.Compaction, compactUntil);
                });

                // check that the compaction had the desired effect
                numExtraEntries = 2;
                Assert.Equal(200 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(compactUntil, log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // stop the service
                await orchestrationService.StopAsync();
            }
            {
                // recover the service
                var service = new NetheriteOrchestrationService(this.settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceQueryClient = (IOrchestrationServiceQueryClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);

                int numExtraEntries = 2;

                // check the log positions
                var log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));
                Assert.Equal(200 * log.FixedRecordSize + numExtraEntries * extraLogEntrySize, log.TailAddress - log.BeginAddress);
                Assert.Equal(compactUntil, log.BeginAddress);
                Assert.Equal(log.ReadOnlyAddress, log.TailAddress);

                // check the instance count
                var instances = await orchestrationServiceQueryClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Equal(100, instances.Count);
              
                // stop the service
                await orchestrationService.StopAsync();
            }
        }

        /// <summary>
        /// Test log compaction that fails right after compaction
        /// </summary>
        [Fact]
        public async Task CompactThenFail()
        {
            this.settings.PartitionCount = 1;
            this.SetCheckpointFrequency(CheckpointFrequency.None);

            var checkpointInjector = this.settings.TestHooks.CheckpointInjector = new Faster.CheckpointInjector(this.settings.TestHooks);

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloFanOutFanIn);
            var orchestrationType2 = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            long currentTail = 0;
            long currentBegin = 0;

            long compactUntil = 0;
            {
                // start the service 
                var service = new NetheriteOrchestrationService(this.settings, this.loggerFactory);
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

                int numExtraEntries = 1;


                // repeat foldover and copy to tail to inflate the log
                for (int i = 0; i < 4; i++)
                {
                    // take a foldover checkpoint
                    var log2 = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.Idle, null));
                    numExtraEntries += 1;
                    Assert.Equal((200 + (100 * i)) * log2.FixedRecordSize + numExtraEntries * extraLogEntrySize, log2.TailAddress - log2.BeginAddress);
                    Assert.Equal(log2.ReadOnlyAddress, log2.TailAddress);

                    currentTail = log2.TailAddress;
                    currentBegin = log2.BeginAddress;

                    // read all instances using point queries to force copy to tail
                    var tasks = instances.Select(instance => orchestrationServiceClient.GetOrchestrationStateAsync(instance.OrchestrationInstance.InstanceId, false));
                    await Task.WhenAll(tasks);
                }

                // do log compaction
                var log = await checkpointInjector.InjectAsync(log =>
                {
                    compactUntil = 500 * log.FixedRecordSize + log.BeginAddress + numExtraEntries * extraLogEntrySize;
                    Assert.Equal(compactUntil, log.SafeReadOnlyAddress);
                    return (Faster.StoreWorker.CheckpointTrigger.Compaction, compactUntil);
                },
                injectFailureAfterCompaction:true);

                await orchestrationService.StopAsync();
            }

            {
                // recover the service 
                var service = new NetheriteOrchestrationService(this.settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                Assert.Equal(this.settings.PartitionCount, (int)service.NumberPartitions);
                var worker = new TaskHubWorker(service);
                var client = new TaskHubClient(service);
                await worker.StartAsync();

                // check that begin and tail are the same
                var log = await checkpointInjector.InjectAsync(log => (Faster.StoreWorker.CheckpointTrigger.None, null));

                Debug.Assert(log.BeginAddress == currentBegin);
                Debug.Assert(log.TailAddress == currentTail);
            }
        }
    }
}