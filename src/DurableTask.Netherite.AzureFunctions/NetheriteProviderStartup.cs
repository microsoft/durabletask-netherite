// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Hosting;
using Microsoft.Extensions.DependencyInjection;

// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: WebJobsStartup(typeof(DurableTask.Netherite.AzureFunctions.NetheriteProviderStartup))]

namespace DurableTask.Netherite.AzureFunctions
{
    class NetheriteProviderStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
#if !NETCOREAPP2_2
            builder.Services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderFactory>();
#else
            builder.Services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderPseudoFactory>();
#endif
            builder.AddDurableTask();
        }
    }
}
