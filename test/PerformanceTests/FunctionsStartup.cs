// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.Functions.Extensions.DependencyInjection.FunctionsStartup(typeof(PerformanceTests.Startup))]
namespace PerformanceTests
{
    using System;
    using DurableTask.Netherite;
    using Microsoft.Azure.Functions.Extensions.DependencyInjection;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using PerformanceTests.Transport;

    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<ConnectionResolver>((s) =>
            {
                var nameResolver = s.GetRequiredService<INameResolver>();
                return new CustomTransportConnectionResolver((name) => nameResolver.Resolve(name));
            });
            builder.Services.AddSingleton<ITransportLayerFactory>((s) =>
            {
                return new TriggerTransportFactory();
            });
        }

        class CustomTransportConnectionResolver : ConnectionNameToConnectionStringResolver
        {
            public CustomTransportConnectionResolver(Func<string,string> resolver) : base(resolver)
            {
            }

            public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
            {
                storageChoice = StorageChoices.Faster;
                transportChoice = TransportChoices.Custom;
            }
        }
    }
}