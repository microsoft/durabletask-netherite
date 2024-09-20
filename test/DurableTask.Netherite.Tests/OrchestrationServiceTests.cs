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
            TestConstants.ValidateEnvironment(requiresTransportSpec: true);
            this.loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(loggerProvider);
        }

        [Fact]
        public Task StopAsync_IsIdempotent()
        {
            return Common.WithTimeoutAsync(TimeSpan.FromMinutes(1), async () =>
            {
                int numStops = 3;
                IOrchestrationService service = TestConstants.GetTestOrchestrationService(this.loggerFactory);
                for (int i = 0; i < numStops; i++)
                {
                    await service.StopAsync();
                }
            });
        }

        [Fact]
        public Task UnstartedService_CanBeSafelyStopped()
        {
            return Common.WithTimeoutAsync(TimeSpan.FromMinutes(1), async () =>
            {
                IOrchestrationService service = TestConstants.GetTestOrchestrationService(this.loggerFactory);
                await service.StopAsync();
            });
        }
    }
}
