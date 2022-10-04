// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.Functions.Extensions.DependencyInjection.FunctionsStartup(typeof(PerformanceTests.Startup))]
namespace PerformanceTests
{
    using DurableTask.Netherite;
    using Microsoft.Azure.Functions.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using PerformanceTests.Transport;

    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<ITransportLayerFactory>((s) =>
            {
                return new TriggerTransportFactory();
            });
        }
    }
}