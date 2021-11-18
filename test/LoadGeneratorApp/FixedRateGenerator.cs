// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Text.Json.Serialization;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    public static class FixedRateGenerator
    {
        public class Input
        {
            public FixedRateParameters Parameters;
            public string ScenarioId;
            public int RobotNumber;
        }

        public static EntityId GetId(string scenarioId, int index)
        {
            return new EntityId(nameof(FixedRateGenerator), $"{scenarioId}-{index/100:D3}!{index%100:D2}");
        }

        [FunctionName(nameof(FixedRateGenerator))]
        public static async Task Invoke(
            [EntityTrigger] IDurableEntityContext dfcontext,
            ILogger logger)
        {
            if (dfcontext.OperationName == "Ping")
            {
                return;
            }

            var input = dfcontext.GetInput<Input>();
         
            //IRobotContext context = new RobotContext(input, logger);
            int robotnumber = input.RobotNumber;
            Random random = new Random(input.Parameters.RandomSeed + robotnumber);
            FixedRateParameters parameters = input.Parameters;
            int numrobots = parameters.Robots;
            int rounds = (int)(parameters.Duration * parameters.Rate);

            logger.LogInformation($"R{robotnumber:D3} Init Client");
            Client client = await Client.AcquireAsync(parameters, logger);

            string CallbackUri(Guid reqId)
                => $"{parameters.GeneratorUrl}/node/{input.RobotNumber%100}/request/{reqId}/callback";

            try
            {
                Dispatcher dispatcher = new Dispatcher(parameters, random, parameters.Robots, client, robotnumber, robotnumber, CallbackUri);
                var period = 1000 / parameters.Rate;

                // delay each robot differently so they run staggered
                await Task.Delay(TimeSpan.FromMilliseconds(period * robotnumber / numrobots));

                logger.LogInformation($"{input.ScenarioId} R{robotnumber:D3} Start Client");

                var ratetimer = new Stopwatch();
                var requesttimer = new Stopwatch();
                var result = new Dictionary<string, List<(DateTime d, double l)>>();
                var sw = new Stopwatch();

                foreach (var m in Enum.GetNames(typeof(Events)))
                {
                    result.Add(m, new List<(DateTime d, double l)>());
                }
                void Record(string metric, DateTime time, double duration)
                {
                    result[metric].Add((time, duration));
                }

                var numrequests = 0;

                ratetimer.Start();

                while (numrequests < rounds && ratetimer.Elapsed.TotalSeconds < parameters.Duration)
                {

                    try
                    {
                        logger.LogDebug($"{input.ScenarioId} R{robotnumber:D3}.{numrequests:D6} Issue Request");
                        Record(nameof(Events.started), DateTime.UtcNow, 0);
                        sw.Restart();

                        var statTuple = await dispatcher.Execute(
                                parameters.Operation,
                                logger
                            ).ConfigureAwait(false);

                        sw.Stop();
                        logger.LogDebug($"{input.ScenarioId} R{robotnumber:D3}.{numrequests:D6} Success");
                        Record(nameof(Events.finished), statTuple.Time ?? DateTime.UtcNow, statTuple.Duration ?? sw.Elapsed.TotalMilliseconds);
                    }
                    catch (TimeoutException e)
                    {
                        sw.Stop();
                        logger.LogWarning($"{input.ScenarioId} R{robotnumber:D3}.{numrequests:D6} Failed with Request Timeout: {e.Message}");
                        Record(nameof(Events.requesttimeout), DateTime.UtcNow, sw.Elapsed.TotalMilliseconds);
                        Record(nameof(Events.failed), DateTime.UtcNow, sw.Elapsed.TotalMilliseconds);
                    }
                    catch (OperationCanceledException)
                    {
                        sw.Stop();
                        logger.LogInformation($"{input.ScenarioId} R{robotnumber:D3}.{numrequests:D6} Request was canceled");
                        Record(nameof(Events.canceled), DateTime.UtcNow, sw.Elapsed.TotalMilliseconds);
                        Record(nameof(Events.failed), DateTime.UtcNow, sw.Elapsed.TotalMilliseconds);
                    }
                    catch (Exception e)
                    {
                        sw.Stop();
                        logger.LogError($"{input.ScenarioId} R{robotnumber:D3}.{numrequests:D6} Failed with Exception: " + e);
                        Record(nameof(Events.exception), DateTime.UtcNow, sw.Elapsed.TotalMilliseconds);
                        Record(nameof(Events.failed), DateTime.UtcNow, sw.Elapsed.TotalMilliseconds);
                    }
                   
                    numrequests++;

                    if (numrequests < rounds)
                    {
                        // rate control: do not issue more than 1 request per period
                        var aheadofrate = (int)(numrequests * period - ratetimer.ElapsedMilliseconds);
                        if (aheadofrate > 0)
                        {
                            await Task.Delay(aheadofrate).ConfigureAwait(false);
                        }
                    }
                }

                logger.LogInformation($"{input.ScenarioId} R{robotnumber:D3} Client Finished");

                dfcontext.Return(result);
            }
            finally
            { 
                await Client.ReleaseAsync(logger);
            }
        }
    }
}
