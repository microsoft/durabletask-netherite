// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.MemoryBalloon
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
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
        [FunctionName(nameof(BalloonEntityInflate))]
        public static async Task<IActionResult> BalloonEntityInflate(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "BalloonEntity/{instanceId}/Inflate/{amount}")] HttpRequest req,
            string instanceId,
            int amount,
            [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId(nameof(BalloonEntity), instanceId);
                await client.SignalEntityAsync(entityId, "inflate", amount);
                return new OkObjectResult($"inflate {amount} was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int) HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(BalloonEntityDeflate))]
        public static async Task<IActionResult> BalloonEntityDeflate(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "BalloonEntity/{instanceId}/Deflate")] HttpRequest req,
          string instanceId,
          [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId(nameof(BalloonEntity), instanceId);
                await client.SignalEntityAsync(entityId, "deflate", null);
                return new OkObjectResult($"deflate was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(MemoryBalloonEntityInflate))]
        public static async Task<IActionResult> MemoryBalloonEntityInflate(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "MemoryBalloonEntity/{instanceId}/Inflate/{amount}")] HttpRequest req,
            string instanceId,
            int amount,
            [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId(nameof(MemoryBalloonEntity), instanceId);
                await client.SignalEntityAsync(entityId, "inflate", amount);
                return new OkObjectResult($"inflate {amount} was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int) HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(MemoryBalloonEntityDeflate))]
        public static async Task<IActionResult> MemoryBalloonEntityDeflate(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "MemoryBalloonEntity/{instanceId}/Deflate")] HttpRequest req,
          string instanceId,
          [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId(nameof(MemoryBalloonEntity), instanceId);
                await client.SignalEntityAsync(entityId, "deflate", null);
                return new OkObjectResult($"deflate was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(BalloonOrchestrationStart))]
        public static async Task<IActionResult> BalloonOrchestrationStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "BalloonOrchestration/{instanceId}/Start")] HttpRequest req,
            string instanceId,
            [DurableClient] IDurableClient client)
        {
            try
            {
                await client.StartNewAsync(nameof(BalloonOrchestration), instanceId);
                return new OkObjectResult($"orchestration {instanceId} was started.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(BalloonOrchestrationInflate))]
        public static async Task<IActionResult> BalloonOrchestrationInflate(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "BalloonOrchestration/{instanceId}/Inflate/{amount}")] HttpRequest req,
            string instanceId,
            int amount,
            [DurableClient] IDurableClient client)
        {
            try
            {
                await client.RaiseEventAsync(instanceId, "signal", amount.ToString());
                return new OkObjectResult($"inflate {amount} was sent to orchestration {instanceId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(BalloonOrchestrationDeflate))]
        public static async Task<IActionResult> BalloonOrchestrationDeflate(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "BalloonOrchestration/{instanceId}/Deflate")] HttpRequest req,
          string instanceId,
          [DurableClient] IDurableClient client)
        {
            try
            {
                await client.RaiseEventAsync(instanceId, "signal", "deflate");
                return new OkObjectResult($"deflate was sent to orchestration {instanceId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
