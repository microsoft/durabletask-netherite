// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.Store
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Newtonsoft.Json;

    public static class HttpTriggers
    {
        [FunctionName(nameof(SetStore))]
        public static async Task<IActionResult> SetStore(
            [HttpTrigger(AuthorizationLevel.Anonymous, "put", "post", Route = "store/{key}")] HttpRequest req,
            string key,
            [DurableClient] IDurableClient client)
        {
            try
            {
                string input = await new StreamReader(req.Body).ReadToEndAsync();
                var entityId = new EntityId(nameof(Store), key);
                int size = int.Parse(input);
                await client.SignalEntityAsync(entityId, "setrandom", size);
                return new OkObjectResult($"SetRandom({size}) was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(SetStoreVector))]
        public static async Task<IActionResult> SetStoreVector(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "storevector/{prefix}/{count}")] HttpRequest req,
            int count,
            string prefix,
            [DurableClient] IDurableClient client)
        {
            try
            {
                string input = await new StreamReader(req.Body).ReadToEndAsync();
                int size = int.Parse(input);
                for (int i = 0; i < count; i++)
                {
                    string key = $"{prefix}{i}";
                    var entityId = new EntityId(nameof(Store), key);
                    await client.SignalEntityAsync(entityId, "setrandom", size);
                }
                return new OkObjectResult($"SetRandom({size}) was sent to {count} entities.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(DeleteStore))]
        public static async Task<IActionResult> DeleteStore(
            [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "store/{key}")] HttpRequest req,
            string key,
            [DurableClient] IDurableClient client)
        {
            try
            {

                var entityId = new EntityId(nameof(Store), key);

                await client.SignalEntityAsync(entityId, "delete");
                return new OkObjectResult($"Delete was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(DeleteStoreVector))]
        public static async Task<IActionResult> DeleteStoreVector(
            [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "storevector/{prefix}/{count}")] HttpRequest req,
            int count,
            string prefix,
            [DurableClient] IDurableClient client)
        {
            try
            {
                string input = await new StreamReader(req.Body).ReadToEndAsync();
                for (int i = 0; i < count; i++)
                {
                    string key = $"{prefix}{i}";
                    var entityId = new EntityId(nameof(Store), key);
                    await client.SignalEntityAsync(entityId, "delete");
                }
                return new OkObjectResult($"Delete was sent to {count} entities.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(GetStore))]
        public static async Task<IActionResult> GetStore(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "store/{key}")] HttpRequest req,
            string key,
            [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId(nameof(Store), key);
                var response = await client.ReadEntityStateAsync<Store>(entityId);

                if (!response.EntityExists)
                {
                    return new NotFoundObjectResult($"no such entity: {entityId}");
                }
                else
                {
                    byte[] bytes = response.EntityState.CurrentValue;
                    return new OkObjectResult($"contains {bytes.Length} bytes");
                }
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

    }
}