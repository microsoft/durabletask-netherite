// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
    public class OrchestrationServiceTests
    {
        readonly ILoggerFactory loggerFactory;

        public OrchestrationServiceTests(ITestOutputHelper outputHelper)
        {
            this.loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider(outputHelper);
            this.loggerFactory.AddProvider(loggerProvider);
        }

        [Fact]
        public async Task StopAsync_IsIdempotent()
        {
            int numStops = 3;
            IOrchestrationService service = TestHelpers.GetTestOrchestrationService(this.loggerFactory);
            for (int i =0; i < numStops; i++)
            {
                await service.StopAsync();
            }
        }

        [Fact]
        public async Task UnstartedService_CanBeSafelyStopped()
        {
            IOrchestrationService service = TestHelpers.GetTestOrchestrationService(this.loggerFactory);
            await service.StopAsync();
        }
    }
}
