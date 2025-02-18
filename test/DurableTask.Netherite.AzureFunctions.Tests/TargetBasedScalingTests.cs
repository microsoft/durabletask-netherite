// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
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
        [MemberData(nameof(Tests))]
        public async Task TargetBasedScalingTest(TestData testData)
        {
            this.orchestrationServiceMock.Setup(m => m.MaxConcurrentTaskActivityWorkItems).Returns(testData.MaxA);
            this.orchestrationServiceMock.Setup(m => m.MaxConcurrentTaskOrchestrationWorkItems).Returns(testData.MaxO);

            if (testData.Current > testData.LoadInfos.Count)
            {
                throw new ArgumentException("invalid test parameter", nameof(testData.Current));
            }

            var loadInformation = new Dictionary<uint, PartitionLoadInfo>();
            for (int i = 0; i < testData.LoadInfos.Count; i++)
            {
                testData.LoadInfos[i].WorkerId = $"worker{Math.Min(i, testData.Current - 1)}";
                loadInformation.Add((uint)i, testData.LoadInfos[i]);
            };
         
            var testMetrics = new Metrics()
            {
                LoadInformation = loadInformation,
                Busy = testData.Busy,
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

            Assert.Equal(testData.Expected, result.TargetWorkerCount);
        }

        public record TestData(
            int Expected,
            List<PartitionLoadInfo> LoadInfos,
            string Busy = "busy",
            int MaxA = 10,
            int MaxO = 10,
            int Current = 1)
        {
        }

        public static TheoryData<TestData> Tests
        {
            get
            {
                var data = new TheoryData<TestData>();

                // if "Busy" is null, the task hub is idle and the expected target is zero
                data.Add(new TestData(Expected: 0, [ Idle, FastForAWhile, NowSlow ], Busy: null));

                // with backlog of activities, target is set to the number of activities divided by the max activities per worker (but capped by partition count)
                data.Add(new TestData(Expected: 1, [Act_5000, Idle, Idle], MaxA: 5000));
                data.Add(new TestData(Expected: 1, [Act_4999, Idle, Idle], MaxA: 5000));
                data.Add(new TestData(Expected: 2, [Act_5001, Idle, Idle], MaxA: 5000));
                data.Add(new TestData(Expected: 10, [Act_5000, Idle, FastOnlyNow, Act_5000, Idle, Idle, Idle, Idle, Idle, Idle, Idle, Idle, Idle, Idle, Idle], MaxA: 1000));
                data.Add(new TestData(Expected: 6, [Act_5000, Idle, FastOnlyNow, Act_5000, Idle, Idle], MaxA: 1000));

                // with load-challenged partitions, target is at least the number of challenged partitions
                data.Add(new TestData(Expected: 4, [Act_5000, Act_5000, Act_5000, Act_5000,         Idle, Idle],            MaxA: 50000));
                data.Add(new TestData(Expected: 4, [Act_5000, Act_5000, NowSlow, NowVerySlow,       FastOnlyNow, FastForAWhile],            MaxA: 50000));
                data.Add(new TestData(Expected: 4, [Act_5000, Act_5000, NowSlow, NowVerySlow,       WasSlow, Partial],      MaxA: 50000));
                data.Add(new TestData(Expected: 4, [Act_5000, Act_5000, NowSlow, Orch_5000,         FastOnlyNow, AlmostIdle],      MaxA: 50000));
                data.Add(new TestData(Expected: 4, [Act_5000, Act_5000, NowSlow, NowVerySlow,       Orch_5000, WasSlow],    MaxA: 50000, MaxO: 50000));

                // scale down: if current is above non-idle partitions, scale down to non-idle partitions
                data.Add(new TestData(Expected: 5, [Act_5000, Act_5000, AlmostIdle, AlmostIdle, AlmostIdle, Idle], Current: 6, MaxA: 5000));
                data.Add(new TestData(Expected: 4, [Act_5000, Act_5000, Partial, Partial, Idle, Idle], Current: 6, MaxA: 5000));

                // scale down below non-idle partitions: dont if some partitions are incomplete or show any slowness; otherwise by 1
                data.Add(new TestData(Expected: 4, [FastForAWhile, FastForAWhile, AlmostIdle, Partial], Current: 4));
                data.Add(new TestData(Expected: 4, [FastForAWhile, FastForAWhile, AlmostIdle, FastOnlyNow], Current: 4));
                data.Add(new TestData(Expected: 4, [FastForAWhile, FastForAWhile, AlmostIdle, WasSlow], Current: 4));
                data.Add(new TestData(Expected: 3, [FastForAWhile, FastForAWhile, AlmostIdle, AlmostIdle], Current: 4));
                data.Add(new TestData(Expected: 3, [FastForAWhile, FastForAWhile, AlmostIdle, FastForAWhile], Current: 4));
                
                return data;
            }
        }

        static PartitionLoadInfo Idle => new PartitionLoadInfo()
        {
            LatencyTrend = "IIIII",
        };

        static PartitionLoadInfo FastOnlyNow => new PartitionLoadInfo()
        {
            LatencyTrend = "MMHML",
        };

        static PartitionLoadInfo FastForAWhile => new PartitionLoadInfo()
        {
            LatencyTrend = "LLLLL",
        };

        static PartitionLoadInfo Partial => new PartitionLoadInfo()
        {
            LatencyTrend = "I",
        };

        static PartitionLoadInfo NowSlow => new PartitionLoadInfo()
        {
            LatencyTrend = "IIIIM",
        };

        static PartitionLoadInfo NowVerySlow => new PartitionLoadInfo()
        {
            LatencyTrend = "IIIIH",
        };

        static PartitionLoadInfo WasSlow => new PartitionLoadInfo()
        {
            LatencyTrend = "MIIII",
        };

        static PartitionLoadInfo AlmostIdle => new PartitionLoadInfo()
        {
            LatencyTrend = "LIIII",
        };

        static PartitionLoadInfo Act_5000 => new PartitionLoadInfo()
        {
            Activities = 5000,
            LatencyTrend = "MMMMM",
        };

        static PartitionLoadInfo Act_4999 => new PartitionLoadInfo()
        {
            Activities = 4999,
            LatencyTrend = "MMMMM",
        };

        static PartitionLoadInfo Act_5001 => new PartitionLoadInfo()
        {
            Activities = 5001,
            LatencyTrend = "MMMMM",
        };

        static PartitionLoadInfo Orch_5000 => new PartitionLoadInfo()
        {
            WorkItems = 5000,
            LatencyTrend = "IIIII",
        };
    }
}
