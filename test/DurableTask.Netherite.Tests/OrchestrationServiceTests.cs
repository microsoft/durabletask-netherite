// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Extensions.Logging;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "true")]
    public class OrchestrationServiceTests
    {
        readonly ILoggerFactory loggerFactory;

        public OrchestrationServiceTests(ITestOutputHelper outputHelper)
        {
            Action<string> output = (string message) => outputHelper.WriteLine(message);
            TestConstants.ValidateEnvironment();
            this.loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(loggerProvider);
        }

        [Fact]
        public async Task StopAsync_IsIdempotent()
        {
            int numStops = 3;
            IOrchestrationService service = TestConstants.GetTestOrchestrationService(this.loggerFactory);
            for (int i =0; i < numStops; i++)
            {
                await service.StopAsync();
            }
        }

        [Fact]
        public async Task UnstartedService_CanBeSafelyStopped()
        {
            IOrchestrationService service = TestConstants.GetTestOrchestrationService(this.loggerFactory);
            await service.StopAsync();
        }
    }
}
