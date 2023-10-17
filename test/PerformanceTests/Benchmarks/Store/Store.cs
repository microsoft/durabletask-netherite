// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.Store
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

    public class Store
    {
        public byte[] CurrentValue { get; set; }

        readonly Random random = new Random();

        public void Set(byte[] value)
        {
            this.CurrentValue = value;
        }

        public void SetRandom(int size)
        {
            this.CurrentValue = new byte[size];
            this.random.NextBytes(this.CurrentValue);
        }

        public byte[] Get()
        {
            return this.CurrentValue;
        }

        public int GetSize()
        {
            return this.CurrentValue.Length;
        }

        [FunctionName(nameof(Store))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
            => ctx.DispatchAsync<Store>();
    }
}
