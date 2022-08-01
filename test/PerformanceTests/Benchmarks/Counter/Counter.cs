// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.Counter
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public class Counter
    {
        [JsonProperty("value")]
        public int CurrentValue { get; set; }

        [JsonProperty("modified")]
        public DateTime LastModified { get; set; }

        public void Add(int amount)
        {
            this.CurrentValue += amount;
            this.LastModified = DateTime.UtcNow;
        }

        public void Reset()
        {
            this.CurrentValue = 0;
            this.LastModified = DateTime.UtcNow;
        }

        public void Crash(DateTime timeStamp)
        {
            // crash if less that 4 seconds have passed since the signal was sent
            // (so that we no longer crash when retrying after recovery)
            if ((DateTime.UtcNow - timeStamp) < TimeSpan.FromSeconds(4))
            {
                System.Environment.Exit(333);
            }
        }

        [FunctionName(nameof(Counter))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
            => ctx.DispatchAsync<Counter>();
    }
}
