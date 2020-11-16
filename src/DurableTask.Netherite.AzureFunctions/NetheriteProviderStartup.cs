// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.Functions.Extensions.DependencyInjection.FunctionsStartup(
    typeof(DurableTask.Netherite.AzureFunctions.NetheriteProviderStartup))]

namespace DurableTask.Netherite.AzureFunctions
{
    using System.Collections.Concurrent;
    using Microsoft.Azure.Functions.Extensions.DependencyInjection;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.DependencyInjection;

    class NetheriteProviderStartup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderFactory>();
        }
    }
}
