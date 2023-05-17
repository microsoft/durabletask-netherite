// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;
    using DurableTask.Netherite.Scaling;
    using DurableTask.Netherite.Tests;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;
    using Microsoft.Extensions.Logging;
    using Moq;
    using Xunit;
    using Xunit.Abstractions;
    using static DurableTask.Netherite.Scaling.ScalingMonitor;

    public class TargetBasedScalingTests : IntegrationTestBase
    {
        readonly ILoggerFactory loggerFactory;
        readonly Mock<NetheriteMetricsProvider> metricsProviderMock;
        readonly Mock<IOrchestrationService> orchestrationServiceMock;

        public TargetBasedScalingTests(ITestOutputHelper output) : base(output)
        {
            this.loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(loggerProvider);

            this.orchestrationServiceMock = new Mock<IOrchestrationService>(MockBehavior.Strict);

            this.metricsProviderMock = new Mock<NetheriteMetricsProvider>(
                MockBehavior.Strict,
                null,
                new ConnectionInfo());
        }

        [Theory]
        [InlineData(10, 10, 2)]
        [InlineData(20, 20, 2)]
        public async void GetDurabilityProviderFactoryTest(int maxConcurrentTaskActivityWorkItems, int maxConcurrentTaskOrchestrationWorkItems, int expectedTargetWorkerCount)
        {
            this.orchestrationServiceMock.Setup(m => m.MaxConcurrentTaskActivityWorkItems).Returns(maxConcurrentTaskActivityWorkItems);
            this.orchestrationServiceMock.Setup(m => m.MaxConcurrentTaskOrchestrationWorkItems).Returns(maxConcurrentTaskOrchestrationWorkItems);

            Dictionary<uint, PartitionLoadInfo> loadInformation = new Dictionary<uint, PartitionLoadInfo>()
            {
                { 1, this.Create("A") },
                { 2, this.Create("B") },
            };

            var testMetrics = new Metrics()
            {
                LoadInformation = loadInformation,
                Busy = "busy",
                Timestamp = DateTime.UtcNow,
            };

            var durabilityProviderMock = new Mock<DurabilityProvider>(
                MockBehavior.Strict,
                "storageProviderName",
                this.orchestrationServiceMock.Object,
                new Mock<IOrchestrationServiceClient>().Object,
                "connectionName");

            this.metricsProviderMock.Setup(m => m.GetMetricsAsync()).ReturnsAsync(testMetrics);

            NetheriteTargetScaler targetScaler = new NetheriteTargetScaler(
                "functionId",
                this.metricsProviderMock.Object,
                durabilityProviderMock.Object);

            TargetScalerResult result = await targetScaler.GetScaleResultAsync(new TargetScalerContext());

            Assert.Equal(expectedTargetWorkerCount, result.TargetWorkerCount);
        }

        PartitionLoadInfo Create(string worker)
        {
            return new PartitionLoadInfo()
            {
                WorkerId = worker,
                Activities = 11,
                CacheMB = 1.1,
                CachePct = 33,
                CommitLogPosition = 64,
                InputQueuePosition = 1231,
                Instances = 3,
                LatencyTrend = "IIIII",
                MissRate = 0.1,
                Outbox = 44,
                Requests = 55,
                Timers = 66,
                Wakeup = DateTime.Parse("2022-10-08T17:00:44.7400082Z").ToUniversalTime(),
                WorkItems = 77
            };
        }
    }
}
