// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.Functions.Extensions.DependencyInjection.FunctionsStartup(typeof(StandaloneClient.Startup))]
namespace StandaloneClient
{
    using System;
    using System.Collections.Generic;
    using Azure.Identity;
    using DurableTask.Netherite;
    using DurableTask.Netherite.AzureFunctions;
    using Microsoft.Azure.Functions.Extensions.DependencyInjection;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask.ContextImplementations;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask.Options;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using System.Linq;
    using Castle.Core.Logging;

    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<ExternalClientFactory>();
        }

        public class ExternalClientFactory
        {
            readonly DurableClientFactory configuredClientFactory;
            readonly string defaultHubName;
            readonly string defaultConnectionName;
            readonly bool isEmulation;

            const string DefaultStorageProviderName = "AzureStorage";
            const string DefaultStorageConnectionName = "AzureWebJobsStorage";

            public ExternalClientFactory(
                IOptions<DurableClientOptions> durableClientOptions,
                IOptions<DurableTaskOptions> durableTaskOptions,
                Microsoft.Extensions.Logging.ILoggerFactory loggerFactory, 
                IServiceProvider serviceProvider)
            {
                // determine the name of the configured storage provider
                bool storageTypeIsConfigured = durableTaskOptions.Value.StorageProvider.TryGetValue("type", out object storageType);
                string storageProviderName = "AzureStorage"; // default storage provider name
                if (storageTypeIsConfigured)
                {
                    storageProviderName = storageType.ToString();
                }

                // find the provider factory for the configured storage provider
                IEnumerable<IDurabilityProviderFactory> providerFactories = serviceProvider.GetServices<IDurabilityProviderFactory>();
                IDurabilityProviderFactory providerFactory = providerFactories.First(f => string.Equals(f.Name, storageProviderName, StringComparison.OrdinalIgnoreCase));

                // create the client factory
                this.configuredClientFactory = new DurableClientFactory(durableClientOptions, durableTaskOptions, providerFactory, loggerFactory);

                // determine the default hub name based on the configuration
                this.defaultHubName = durableTaskOptions.Value.HubName;

                // determine the default connection name based on the configuration
                if (durableTaskOptions.Value.StorageProvider.TryGetValue("StorageConnectionName", out object value))
                {
                    this.defaultConnectionName = value.ToString();
                }
                else
                {
                    this.defaultConnectionName = DefaultStorageConnectionName;
                }
            }

            public IDurableClient GetClient(string connectionName = null, string hubName = null)
            {
                var clientOptions = new DurableClientOptions() {
                    ConnectionName = connectionName ?? this.defaultConnectionName, 
                    TaskHub = hubName ?? this.defaultHubName, 
                    IsExternalClient = true,
                };

                var client = this.configuredClientFactory.CreateClient(clientOptions);
                return client;
            }
        }
    }
}