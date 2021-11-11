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
            settings.PartitionManagement = PartitionManagementOptions.Scripted;
            settings.PartitionCount = 1;
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";
            var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
            await service.CreateAsync();
            await service.StartAsync();
            var host = (TransportAbstraction.IHost)service;
            Assert.Equal(1u, service.NumberPartitions);
            await service.StopAsync();
        }
    }

}