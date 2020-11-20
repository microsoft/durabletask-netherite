namespace PerformanceTests.Orchestrations
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public static class CounterTest
    {
        class Input
        {
            public string Key { get; set; }
            public int Expected { get; set; }
        }

        [FunctionName(nameof(WaitForCount))]
        public static async Task<IActionResult> WaitForCount(
            [HttpTrigger(AuthorizationLevel.Function, methods: "post", Route = nameof(WaitForCount))] HttpRequest req,
            [DurableClient] IDurableClient client)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var input = JsonConvert.DeserializeObject<Input>(requestBody);
            var entityId = new EntityId("Counter", input.Key);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            // poll the entity until the expected count is reached
            while (stopwatch.Elapsed < TimeSpan.FromMinutes(5))
            {
                var response = await client.ReadEntityStateAsync<Counter>(entityId);

                if (response.EntityExists
                    && response.EntityState.CurrentValue >= input.Expected)
                {
                    return new OkObjectResult($"{JsonConvert.SerializeObject(response.EntityState)}\n");
                }

                await Task.Delay(TimeSpan.FromSeconds(2));
            }

            return new OkObjectResult("timed out.\n");
        }

        [FunctionName(nameof(Increment))]
        public static async Task<IActionResult> Increment(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = nameof(Increment))] HttpRequest req,
            [DurableClient] IDurableClient client)
        {
            string entityKey = await new StreamReader(req.Body).ReadToEndAsync();
            var entityId = new EntityId("Counter", entityKey);
            await client.SignalEntityAsync(entityId, "add", 1);
            return new OkObjectResult($"increment was sent to {entityId}.\n");
        }
    }

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

        public (int, DateTime) Get() => (this.CurrentValue, DateTime.UtcNow);

        [FunctionName(nameof(Counter))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
            => ctx.DispatchAsync<Counter>();
    }
}
