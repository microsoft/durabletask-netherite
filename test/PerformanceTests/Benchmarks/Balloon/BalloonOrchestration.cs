// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.MemoryBalloon
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public static class BalloonOrchestration
    {
        [FunctionName(nameof(BalloonOrchestration))]
        public static async Task Run([OrchestrationTrigger] IDurableOrchestrationContext ctx, ILogger logger)
        {
            var balloon = new List<byte[]>();

            while (true)
            {
                var signal = await ctx.WaitForExternalEvent<string>("signal");
                if (signal == "deflate")
                {
                    balloon.Clear();
                }
                else 
                {
                    int amount = int.Parse(signal);
                    for (int i = 0; i < amount; i++)
                    {
                        // add approx. 1 MB to the memory
                        balloon.Add(new byte[1024 * 1024]);
                    }
                }

                ctx.SetCustomStatus(new { size = balloon.Count });
            }
        }        
    }
}
