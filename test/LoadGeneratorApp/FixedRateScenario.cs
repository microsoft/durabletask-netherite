// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Azure.WebJobs.Host;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public static class FixedRateScenario
    {
        public class Input
        {
            public string Testname;
            public string ScenarioId;
            public FixedRateParameters Parameters;
        }
       

        [FunctionName(nameof(FixedRateScenario))]
        public static async Task<Results> Run(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            var input = context.GetInput<Input>();
            var parameters = input.Parameters;
            JObject results = new JObject();

            // ----- first, ping all robots to make sure all partitions are ready and running

            var pingtasks = new Task[parameters.Robots];
            for (int i = 0; i < parameters.Robots; i++)
            {
                pingtasks[i] = context.CallEntityAsync(
                    FixedRateGenerator.GetId(input.ScenarioId, i), 
                    "Ping");
            }
            await Task.WhenAll(pingtasks);

            // ----- then, run the load generation on each robot

            DateTime startTime = context.CurrentUtcDateTime;

            var runtasks = new Task<Dictionary<string, List<(DateTime d, double l)>>>[parameters.Robots];
            for (int i = 0; i < parameters.Robots; i++)
            {
                runtasks[i] = context.CallEntityAsync<Dictionary<string, List<(DateTime d, double l)>>>(
                    FixedRateGenerator.GetId(input.ScenarioId, i),
                    "Run", 
                    new FixedRateGenerator.Input()
                    {
                        Parameters = parameters,
                        ScenarioId = input.ScenarioId,
                        RobotNumber = i,
                    });
            }
            await Task.WhenAll(runtasks);

            // ----- process the results

            // collect successful latencies
            var successes = new List<(double lat, double time)>();

            // count outcomes by metric
            var count = new Dictionary<string, int>();
            int dropped = 0;

            foreach (var m in Enum.GetNames(typeof(Events)))
            {
                count.Add(m, 0);
            }

            for (int i = 0; i < runtasks.Length; i++)
            {
                Dictionary<string, List<(DateTime time, double latency)>> response = runtasks[i].Result;

                foreach (var m in Enum.GetNames(typeof(Events)))
                {
                    count[m] = count[m] + response[m].Count;

                    if (m == nameof(Events.finished))
                    {
                        foreach (var x in response[m])
                        {
                            TimeSpan starttime = (x.Item1 - startTime);
                            if ((parameters.Warmup == 0 || starttime > TimeSpan.FromSeconds(parameters.Warmup)) && 
                                (parameters.Cooldown == 0 || starttime < TimeSpan.FromSeconds(parameters.Duration - parameters.Cooldown)))
                            {
                                successes.Add((x.Item2, starttime.TotalSeconds));
                            }
                            else
                            {
                                dropped++;
                            }
                        }
                    }
                }
            }

            // construct eCDF for the successful results
            successes.Sort();
            var eCDF = new JArray();
            for (int i = 0; i < successes.Count; i++)
            {
                eCDF.Add(new JArray(successes[i].time.ToString("F3"), successes[i].lat.ToString("F2"), (((double)i) / successes.Count).ToString("F3")));
            }

            // record the tuples and the summary in the results
            results["summary"] = JToken.Parse(JsonConvert.SerializeObject(count));
            results["tuples"] = eCDF;
            results["collected"] = eCDF.Count;
            results["dropped"] = dropped;

            // ----- save the results to a json blob

            var resultObject = new Results()
            {
                parameters = parameters,
                coordinates = new Results.Coordinates()
                {
                    id = input.ScenarioId,
                    scenarioname = parameters.ToString(),
                    testname = input.Testname,
                },
                results = results,
            };

            await context.CallActivityAsync(nameof(Results.SaveResults), resultObject);

            return resultObject;
        }
    }
}