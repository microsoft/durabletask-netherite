namespace PerformanceTests.Transport
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System.Net;
    using Dynamitey.DynamicObjects;
    using System.Collections.Generic;
    using System.Web.Http;

    public static class TransportHttp
    {
        static IActionResult ErrorResult(Exception exception, string context, ILogger logger)
        {
            logger.LogError(exception, $"exception in {context}");
            return new ObjectResult($"exception in {context}: {exception}") { StatusCode = (int)HttpStatusCode.InternalServerError };
        }

        [FunctionName(nameof(StartAll))]
        public static async Task<IActionResult> StartAll(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/startall")] HttpRequest req,
            TriggerTransportFactory transportFactory,
            ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string[] hosts = JsonConvert.DeserializeObject<string[]>(requestBody);
                await transportFactory.Instance.StartAllAsync(hosts);
                return new OkResult();
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(StartAll), log);
            }
        }

        [FunctionName(nameof(StartLocal))]
        public static async Task<IActionResult> StartLocal(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/startlocal/{index}")] HttpRequest req,
            int index,
            TriggerTransportFactory transportFactory,
            ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string[] hosts = JsonConvert.DeserializeObject<string[]>(requestBody);
                await transportFactory.Instance.StartLocalAsync(hosts, index);
                return new OkResult();
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(StartLocal), log);
            }
        }

        [FunctionName(nameof(GetClientId))]
        public static async Task<IActionResult> GetClientId(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "triggertransport/client")] HttpRequest req,
            TriggerTransportFactory transportFactory,
            ILogger log)
        {
            try
            {
                await transportFactory.Instance.WhenOrchestrationServiceStarted;
                return new OkObjectResult(new { transportFactory.Instance.ClientId });
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(GetClientId), log);
            }
        }

        [FunctionName(nameof(DeliverToPartition))]
        public static async Task<IActionResult> DeliverToPartition(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/partition/{partitionId}")] HttpRequest req,
            int partitionId,
            TriggerTransportFactory transportFactory,
            ILogger log)
        {
            try
            {
                await transportFactory.Instance.WhenLocallyStarted;
                await transportFactory.Instance.DeliverToPartition(partitionId, req.Body);
                return new OkResult();
            }
            catch(Exception e)
            {
                return ErrorResult(e, nameof(DeliverToPartition), log);
            }
        }

        [FunctionName(nameof(DeliverToClient))]
        public static async Task<IActionResult> DeliverToClient(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/client/{clientId}")] HttpRequest req,
            Guid clientId,
            TriggerTransportFactory transportFactory,
            ILogger log)
        {
            try
            {
                await transportFactory.Instance.WhenLocallyStarted;
                await transportFactory.Instance.DeliverToClient(clientId, req.Body);
                return new OkResult();
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(DeliverToClient), log);
            }
        }

        [FunctionName(nameof(DeliverToLoadMonitor))]
        public static async Task<IActionResult> DeliverToLoadMonitor(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/loadmonitor")] HttpRequest req,
            TriggerTransportFactory transportFactory,
            ILogger log)
        {
            try
            {
                await transportFactory.Instance.WhenLocallyStarted;
                await transportFactory.Instance.DeliverToLoadMonitor(req.Body);
                return new OkResult();
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(DeliverToLoadMonitor), log);
            }
        }
    }
}
