// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace ScalingTests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Netherite;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    class Program
    {
        static async Task Main(string[] args)
        {
            var settings = new NetheriteOrchestrationServiceSettings()
            {
                HubName = "perftests",
                PartitionManagement = PartitionManagementOptions.ClientOnly,
                LogLevelLimit = LogLevel.Trace,
                EventLogLevelLimit = LogLevel.Trace,
                StorageLogLevelLimit = LogLevel.Trace,
                TransportLogLevelLimit = LogLevel.Trace,
                WorkItemLogLevelLimit = LogLevel.Trace,
            };
            settings.Validate((string connectionName) => Environment.GetEnvironmentVariable(connectionName));
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddProvider(new ConsoleLoggerProvider());
            var logger = loggerFactory.CreateLogger("Main");

            logger.LogInformation("Starting OrchestrationService...");
            var service = new NetheriteOrchestrationService(settings, loggerFactory);
            var orchestrationService = (IOrchestrationService)service;
            await orchestrationService.StartAsync();

            if (service.TryGetScalingMonitor(out ScalingMonitor scalingMonitor))
            {
                while (true)
                {
                    logger.LogInformation("Hit a number  to run scale decision for that workercount, or 'q' to exit");
                    ConsoleKeyInfo keyInfo = Console.ReadKey();
                    int workerCount;

                    if (keyInfo.KeyChar == 'q')
                    {
                        break;
                    }
                    else if (!int.TryParse($"{keyInfo.KeyChar}", out workerCount))
                    {
                        workerCount = 1;
                    }

                    logger.LogInformation("--------- Collecting Metrics...");
                    var metrics = await scalingMonitor.CollectMetrics();
                    logger.LogInformation(JsonConvert.SerializeObject(metrics, Formatting.Indented));
                    logger.LogInformation($"--------- Making scale decision for worker count {workerCount}...");
                    var decision = scalingMonitor.GetScaleRecommendation(workerCount, metrics);
                    logger.LogInformation(JsonConvert.SerializeObject(decision, Formatting.Indented));
                }
            }
            else
            {
                logger.LogError("failed to create scaling monitor.");
            } 

            // shut down
            logger.LogInformation("OchestrationService stopping...");
            await orchestrationService.StopAsync();
            logger.LogInformation("Done");
        }
    }
}
