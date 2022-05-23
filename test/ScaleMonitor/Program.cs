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

            ScalingMonitor scalingMonitor = new ScalingMonitor(
                        Environment.GetEnvironmentVariable("AzureWebJobsStorage"),
                        Environment.GetEnvironmentVariable("EventHubsConnection"),
                        "DurableTaskPartitions",
                         "perftests",
                         (a, b, c) => Console.Out.WriteLine("Recommendation: {a} {b} {c}"),
                         (a) => Console.Out.WriteLine("Information: {a}"),
                         (a, b) => Console.Out.WriteLine("Error: {a} {b}"));

            while (true)
            {
                Console.Out.WriteLine("Hit a number  to run scale decision for that workercount, or 'q' to exit");
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

                Console.Out.WriteLine("--------- Collecting Metrics...");
                ScalingMonitor.Metrics metrics = default;
                try
                {
                    metrics = await scalingMonitor.CollectMetrics();
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine($"Caught exception: {e}");
                    continue;
                }
                Console.Out.WriteLine(JsonConvert.SerializeObject(metrics, Formatting.Indented));
                Console.Out.WriteLine($"--------- Making scale decision for worker count {workerCount}...");
                var decision = scalingMonitor.GetScaleRecommendation(workerCount, metrics);
                Console.Out.WriteLine(JsonConvert.SerializeObject(decision, Formatting.Indented));
            }
        }
    }
}
