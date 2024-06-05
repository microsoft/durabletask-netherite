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
    using Microsoft.Extensions.DependencyInjection.Extensions;

    class NetheriteProviderStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
#if !NETCOREAPP2_2
            // We use the UnambiguousNetheriteProviderFactory class instead of the base NetheriteProviderFactory class
            // to avoid ambiguous constructor errors during DI. More details for this workaround can be found in the UnambiguousNetheriteProviderFactory class.
            builder.Services.AddSingleton<IDurabilityProviderFactory, UnambiguousNetheriteProviderFactory>();
            builder.Services.TryAddSingleton<ConnectionResolver, NameResolverBasedConnectionNameResolver>();
#else
            builder.Services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderPseudoFactory>();
#endif
        }
    }
}
