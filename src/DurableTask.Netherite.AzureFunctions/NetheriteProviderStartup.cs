// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.WebJobs.Hosting.WebJobsStartup(typeof(DurableTask.Netherite.AzureFunctions.NetheriteProviderStartup))]

namespace DurableTask.Netherite.AzureFunctions
{
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Hosting;
    using Microsoft.Extensions.DependencyInjection;

    class NetheriteProviderStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
#if !NETCOREAPP2_2
            builder.Services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderFactory>();
#else
            builder.Services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderPseudoFactory>();
#endif
        }
    }
}
