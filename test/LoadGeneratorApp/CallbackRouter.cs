namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Text.Json.Serialization;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    // Copyright (c) Microsoft Corporation.
    // Licensed under the MIT License.

    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    public static class CallbackRouter
    {
        public static EntityId GetId(int node)
        {
            return new EntityId(nameof(CallbackRouter), $"!{node%100:D2}");
        }

        [FunctionName(nameof(CallbackRouter))]
        public static void Invoke(
            [EntityTrigger] IDurableEntityContext dfcontext,
            ILogger logger)
        {
            var input = dfcontext.GetInput<(Guid reqId, string content)>();
            Client.DeliverCallback(input.reqId, input.content);
        }
    }
}
