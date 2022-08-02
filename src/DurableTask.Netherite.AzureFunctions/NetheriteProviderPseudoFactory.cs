// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Executors;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;

    public class NetheriteProviderPseudoFactory : IDurabilityProviderFactory
    { 
        public const string ProviderName = "Netherite";
        public string Name => ProviderName;

        // Called by the Azure Functions runtime dependency injection infrastructure
        public NetheriteProviderPseudoFactory(
            IOptions<DurableTaskOptions> extensionOptions,
            ILoggerFactory loggerFactory,
#pragma warning disable CS0618 // Type or member is obsolete
            IConnectionStringResolver connectionStringResolver,
#pragma warning restore CS0618 // Type or member is obsolete
            IHostIdProvider hostIdProvider,
            INameResolver nameResolver,
#pragma warning disable CS0612 // Type or member is obsolete
            IPlatformInformation platformInfo)
#pragma warning restore CS0612 // Type or member is obsolete
        {
        }

        /// <inheritdoc/>
        public DurabilityProvider GetDurabilityProvider()
        {
            throw new Exception("The Netherite storage provider is not supported on netcoreapp2. Please use netcoreapp3.1 or later.");
        }

        // Called by the Durable client binding infrastructure
        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            throw new Exception("The Netherite storage provider is not supported on netcoreapp2. Please use netcoreapp3.1 or later.");
        }
    }
}
