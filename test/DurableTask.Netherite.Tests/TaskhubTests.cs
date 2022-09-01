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
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "false")]
    public class TaskhubTests : IDisposable
    {
        readonly HostFixture.TestTraceListener traceListener;
        readonly ILoggerFactory loggerFactory;
        readonly XunitLoggerProvider provider;
        readonly Action<string> output;
        ITestOutputHelper outputHelper;

        public TaskhubTests(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            this.output = (string message) => this.outputHelper?.WriteLine(message);

            this.loggerFactory = new LoggerFactory();
            this.provider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(this.provider);
            this.traceListener = new HostFixture.TestTraceListener();
            Trace.Listeners.Add(this.traceListener);
            this.traceListener.Output = this.output;
            TestConstants.ValidateEnvironment(requiresTransportSpec: true);
        }

        public void Dispose()
        {
            this.outputHelper = null;
            Trace.Listeners.Remove(this.traceListener);
        }

        /// <summary>
        /// Create a taskhub, delete it, and create it again.
        /// </summary>
        [Theory]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task CreateDeleteCreate(bool singleHost, bool deleteTwice)
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings(singleHost ? "SingleHost" : null);
            settings.HubName = $"{nameof(TaskhubTests)}-{Guid.NewGuid()}";

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceQueryClient)service;
                await orchestrationService.CreateAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                var client = new TaskHubClient(service);

                // run a query
                var states = await orchestrationServiceClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Empty(states);

                // stop and delete the service
                await orchestrationService.StopAsync();
                await orchestrationService.DeleteAsync();

                if (deleteTwice)
                {
                    // delete again, should be idempotent
                    await orchestrationService.DeleteAsync();
                }
            }

            {
                // run the service a second time
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                var orchestrationService = (IOrchestrationService)service;
                var orchestrationServiceClient = (IOrchestrationServiceQueryClient)service;
                await orchestrationService.CreateIfNotExistsAsync();
                await orchestrationService.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                var client = new TaskHubClient(service);

                // run a query
                var states = await orchestrationServiceClient.GetAllOrchestrationStatesAsync(CancellationToken.None);
                Assert.Empty(states);

                // stop and delete the service again
                await orchestrationService.StopAsync();
                await orchestrationService.DeleteAsync();
            }
        }
    }
}