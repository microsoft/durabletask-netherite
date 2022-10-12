// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.Functions.Extensions.DependencyInjection.FunctionsStartup(typeof(PerformanceTests.Startup))]
namespace PerformanceTests
{
    using System;
    using Azure.Identity;
    using DurableTask.Netherite;
    using DurableTask.Netherite.AzureFunctions;
    using Microsoft.Azure.Functions.Extensions.DependencyInjection;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;

    public class Startup : FunctionsStartup
    {
        public class MyConnectionResolver : CredentialBasedConnectionNameResolver
        {
            readonly INameResolver nameResolver;

            public MyConnectionResolver(INameResolver nameResolver) : base(new DefaultAzureCredential()) 
            {
                this.nameResolver = nameResolver;
            }
            
            string Resolve(string name)  =>
                this.nameResolver.Resolve(name) 
                ?? Environment.GetEnvironmentVariable(name)
                ?? throw new InvalidOperationException($"missing configuration setting '{name}'");

            public override string GetStorageAccountName(string connectionName) => this.Resolve($"{connectionName}__accountName");

            public override string GetEventHubsNamespaceName(string connectionName) => this.Resolve($"{connectionName}__eventHubsNamespaceName");
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<DurableTask.Netherite.ConnectionResolver>(
                (IServiceProvider serviceProvider) => new MyConnectionResolver(serviceProvider.GetRequiredService<INameResolver>()));
        }
    }
}