// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
{
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    public static class Activities
    {
        [FunctionName(nameof(SayHello))]
        public static string SayHello([ActivityTrigger] string name) => $"Hello {name}!";
    }
}
