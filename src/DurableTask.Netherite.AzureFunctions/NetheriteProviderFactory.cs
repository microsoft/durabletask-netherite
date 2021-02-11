// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using DurableTask.Core;
    using DurableTask.Netherite;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Executors;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Newtonsoft.Json;

    public class NetheriteProviderFactory : IDurabilityProviderFactory
    {
        readonly ConcurrentDictionary<DurableClientAttribute, NetheriteProvider> cachedProviders
            = new ConcurrentDictionary<DurableClientAttribute, NetheriteProvider>();

        readonly DurableTaskOptions extensionOptions;
        readonly IConnectionStringResolver connectionStringResolver;
        readonly IHostIdProvider hostIdProvider;

        // the following are boolean options that can be specified in host.json,
        // but are not passed on to the backend
        public bool TraceToConsole { get; }
        public bool TraceToBlob { get; }
     
        NetheriteProvider defaultProvider;
        ILoggerFactory loggerFactory;

        internal static BlobLogger BlobLogger { get; set; }

        // Called by the Azure Functions runtime dependency injection infrastructure
        public NetheriteProviderFactory(
            IOptions<DurableTaskOptions> extensionOptions,
            ILoggerFactory loggerFactory,
            IConnectionStringResolver connectionStringResolver,
            IHostIdProvider hostIdProvider)
        {
            this.extensionOptions = extensionOptions?.Value ?? throw new ArgumentNullException(nameof(extensionOptions));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.connectionStringResolver = connectionStringResolver ?? throw new ArgumentNullException(nameof(connectionStringResolver));
            this.hostIdProvider = hostIdProvider;

            bool ReadBooleanSetting(string name) => this.extensionOptions.StorageProvider.TryGetValue(name, out object objValue)
                && objValue is string stringValue && bool.TryParse(stringValue, out bool boolValue) && boolValue;

            this.TraceToConsole = ReadBooleanSetting(nameof(this.TraceToConsole));
            this.TraceToBlob = ReadBooleanSetting(nameof(this.TraceToBlob));
        }

        NetheriteOrchestrationServiceSettings GetNetheriteOrchestrationServiceSettings(string taskHubNameOverride = null)
        {
            var eventSourcedSettings = new NetheriteOrchestrationServiceSettings();

            // override DTFx defaults to the defaults we want to use in DF
            eventSourcedSettings.ThrowExceptionOnInvalidDedupeStatus = true;

            // copy all applicable fields from both the options and the storageProvider options
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.extensionOptions), eventSourcedSettings);
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.extensionOptions.StorageProvider), eventSourcedSettings);
 
            // if worker id is specified in environment, it overrides the configured setting
            string workerId = Environment.GetEnvironmentVariable("WorkerId");
            if (!string.IsNullOrEmpty(workerId))
            {
                if (workerId == "HostId")
                {
                    workerId = this.hostIdProvider.GetHostIdAsync(CancellationToken.None).GetAwaiter().GetResult();
                }
                eventSourcedSettings.WorkerId = workerId;
            }

            eventSourcedSettings.HubName = this.extensionOptions.HubName;

            if (taskHubNameOverride != null)
            {
                eventSourcedSettings.HubName = taskHubNameOverride;
            }

            eventSourcedSettings.Validate((name) => this.connectionStringResolver.Resolve(name));

            if (this.TraceToConsole || this.TraceToBlob)
            {
                // capture trace events generated in the backend and redirect them to additional sinks
                this.loggerFactory = new LoggerFactoryWrapper(this.loggerFactory, eventSourcedSettings.HubName, eventSourcedSettings.WorkerId, this);
            }

            return eventSourcedSettings;
        }

        /// <inheritdoc/>
        public DurabilityProvider GetDurabilityProvider()
        {
            if (this.defaultProvider == null)
            {
                var settings = this.GetNetheriteOrchestrationServiceSettings();

                if (this.TraceToBlob && BlobLogger == null)
                {
                    BlobLogger = new BlobLogger(settings.ResolvedStorageConnectionString, settings.HubName, settings.WorkerId);
                }

                var key = new DurableClientAttribute()
                {
                    TaskHub = settings.HubName,
                    ConnectionName = settings.ResolvedStorageConnectionString,
                };
 
                this.defaultProvider = this.cachedProviders.GetOrAdd(key, _ =>
                {
                    var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                    return new NetheriteProvider(service, settings);
                });
            }

            return this.defaultProvider;
        }

        // Called by the Durable client binding infrastructure
        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            var settings = this.GetNetheriteOrchestrationServiceSettings(attribute.TaskHub);

            if (string.Equals(this.defaultProvider.Settings.HubName, settings.HubName, StringComparison.OrdinalIgnoreCase) &&
                 string.Equals(this.defaultProvider.Settings.ResolvedStorageConnectionString, settings.ResolvedStorageConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return this.defaultProvider;
            }

            DurableClientAttribute key = new DurableClientAttribute()
            {
                TaskHub = settings.HubName,
                ConnectionName = settings.ResolvedStorageConnectionString,
            };

            return this.cachedProviders.GetOrAdd(key, _ =>
            {
                //TODO support client-only version
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                return new NetheriteProvider(service, settings);
            });
        }

    }
}
