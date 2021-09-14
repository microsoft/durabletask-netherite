// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
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

    public static class Heap
    {
        readonly static double megabytesFactor = 1.0 / (1024 * 1024);

        [FunctionName(nameof(HeapCollect))]
        public static IActionResult HeapCollect(
         [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "heap/collect")] HttpRequest req,
         [DurableClient] IDurableClient client)
        {
            try
            {
                long before = GC.GetGCMemoryInfo().HeapSizeBytes;
                GC.Collect();
                long after = GC.GetGCMemoryInfo().HeapSizeBytes;

                return new JsonResult(new
                {
                    collected = (megabytesFactor * (before - after)).ToString("F2"),
                    before = (megabytesFactor * before).ToString("F2"),
                    heapsize = (megabytesFactor * after).ToString("F2"),
                });
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(HeapSize))]
        public static IActionResult HeapSize(
         [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "heap")] HttpRequest req,
         [DurableClient] IDurableClient client)
        {
            try
            {
                long after = GC.GetGCMemoryInfo().HeapSizeBytes;

                return new JsonResult(new
                {
                    heapsize = (megabytesFactor * after).ToString("F2"),
                });
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}
