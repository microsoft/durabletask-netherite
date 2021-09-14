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

    public class BalloonEntity
    {
        [JsonProperty]
        public List<byte[]> Balloon { get; set; } = new List<byte[]>();

        static ILogger logger;

        public BalloonEntity(ILogger logger)
        {
            BalloonEntity.logger = logger;
        }

        public void Inflate(int amount)
        {
            for (int i = 0; i < amount; i++)
            {
                // add approx. 1 MB to the memory
                this.Balloon.Add(new byte[1024 * 1024]);
            }
        }

        public void Deflate()
        {
            this.Balloon.Clear();
        }

        [FunctionName(nameof(BalloonEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx, ILogger logger)
            => ctx.DispatchAsync<BalloonEntity>(logger);
    }
}
