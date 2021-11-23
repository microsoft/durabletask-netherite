// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;
    using static DurableTask.Netherite.Tests.SingleHostFixture;

    [Collection("NetheriteTests")]
    public class FasterPartitionTests : IDisposable
    {
        readonly TestTraceListener traceListener;
        readonly ILoggerFactory loggerFactory;
        readonly XunitLoggerProvider provider;


        public FasterPartitionTests(ITestOutputHelper output)
        {
            this.loggerFactory = new LoggerFactory();
            this.provider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(this.provider);
            this.traceListener = new TestTraceListener();
            Trace.Listeners.Add(this.traceListener);
            this.provider.Output = output;
            this.traceListener.Output = output;
        }

        public void Dispose()
        {
            this.provider.Output = null;
            this.traceListener.Output = null;
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
                await service.CreateAsync();
                await service.StartAsync();
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
                await service.StopAsync();
            }
            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var client = new TaskHubClient(service);

                var orchestrationState = await client.GetOrchestrationStateAsync("0");
                Assert.Equal(OrchestrationStatus.Completed, orchestrationState.OrchestrationStatus);

                // stop the service
                await service.StopAsync();
            }
        }

        /// <summary>
        /// Create a partition and then restore it.
        /// </summary>
        [Fact]
        public async Task Locality()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {  
                StoreLogPageSizeBits = 10,       // 1 KB
                StoreLogMemorySizeBits = 14,     // 16 KB, which amounts to about 666 entries
            };

            // don't take any extra checkpoints
            settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 *1024 * 1024;
            settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
            settings.IdleCheckpointFrequencyMs = (long) TimeSpan.FromDays(1).TotalMilliseconds;

            //settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";
            settings.HubName = $"{TestConstants.TaskHubName}-Locality";

            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 500;

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
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
                    await Task.WhenAll(tasks);
                }

                // wait for all orchestrations
                {
                    var tasks = new Task[OrchestrationCount];
                    for (int i = 0; i < OrchestrationCount; i++)
                        tasks[i] = client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(10));
                    await Task.WhenAll(tasks);
                }

                // stop the service
                await service.StopAsync();
            }
        }

        /// <summary>
        /// Create a partition and then restore it.
        /// </summary>
        [Fact]
        public async Task Locality2()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;

            // don't take any extra checkpoints
            settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 * 1024 * 1024;
            settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
            settings.IdleCheckpointFrequencyMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;

            //settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";
            settings.HubName = $"{TestConstants.TaskHubName}-Locality";

            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 1000;

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var client = new TaskHubClient(service);

                // wait for all orchestrations
                {
                    var tasks = new Task[OrchestrationCount];
                    for (int i = 0; i < OrchestrationCount; i++)
                        tasks[i] = client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(10));
                    await Task.WhenAll(tasks);
                }

                // stop the service
                await service.StopAsync();
            }
        }
    }

}