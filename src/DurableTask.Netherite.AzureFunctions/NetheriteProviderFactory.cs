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
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Executors;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Newtonsoft.Json;

    public class NetheriteProviderFactory : IDurabilityProviderFactory
    {
        readonly ConcurrentDictionary<DurableClientAttribute, NetheriteProvider> cachedProviders
            = new ConcurrentDictionary<DurableClientAttribute, NetheriteProvider>();

        readonly DurableTaskOptions options;
        readonly INameResolver nameResolver;
        readonly IHostIdProvider hostIdProvider;

        readonly bool inConsumption;
        
        // the following are boolean options that can be specified in host.json,
        // but are not passed on to the backend
        public bool TraceToConsole { get; }
        public bool TraceToBlob { get; }
     
        NetheriteProvider defaultProvider;
        ILoggerFactory loggerFactory;

        internal static BlobLogger BlobLogger { get; set; }

        public const string ProviderName = "Netherite";
        public string Name => ProviderName;

        // Called by the Azure Functions runtime dependency injection infrastructure
        public NetheriteProviderFactory(
            IOptions<DurableTaskOptions> extensionOptions,
            ILoggerFactory loggerFactory,
            IConnectionStringResolver connectionStringResolver,
            IHostIdProvider hostIdProvider,
            INameResolver nameResolver,
#pragma warning disable CS0612 // Type or member is obsolete
            IPlatformInformationService platformInfo)
#pragma warning restore CS0612 // Type or member is obsolete
        {
            this.options = extensionOptions?.Value ?? throw new ArgumentNullException(nameof(extensionOptions));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.nameResolver = nameResolver ?? throw new ArgumentNullException(nameof(nameResolver));

            this.hostIdProvider = hostIdProvider;
            this.inConsumption = platformInfo.InConsumption();

            bool ReadBooleanSetting(string name) => this.options.StorageProvider.TryGetValue(name, out object objValue)
                && objValue is string stringValue && bool.TryParse(stringValue, out bool boolValue) && boolValue;

            this.TraceToConsole = ReadBooleanSetting(nameof(this.TraceToConsole));
            this.TraceToBlob = ReadBooleanSetting(nameof(this.TraceToBlob));
        }

        NetheriteOrchestrationServiceSettings GetNetheriteOrchestrationServiceSettings(string taskHubNameOverride = null)
        {
            var eventSourcedSettings = new NetheriteOrchestrationServiceSettings();

            // override DTFx defaults to the defaults we want to use in DF
            eventSourcedSettings.ThrowExceptionOnInvalidDedupeStatus = true;

            // The consumption plan has different performance characteristics so we provide
            // different defaults for key configuration values.
            int maxConcurrentOrchestratorsDefault = this.inConsumption ? 5 : 10 * Environment.ProcessorCount;
            int maxConcurrentActivitiesDefault = this.inConsumption ? 10 : 10 * Environment.ProcessorCount;

            // The following defaults are only applied if the customer did not explicitely set them on `host.json`
            this.options.MaxConcurrentOrchestratorFunctions = this.options.MaxConcurrentOrchestratorFunctions ?? maxConcurrentOrchestratorsDefault;
            this.options.MaxConcurrentActivityFunctions = this.options.MaxConcurrentActivityFunctions ?? maxConcurrentActivitiesDefault;

            // copy all applicable fields from both the options and the storageProvider options
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options), eventSourcedSettings);
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options.StorageProvider), eventSourcedSettings);
 
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

            eventSourcedSettings.HubName = this.options.HubName;

            if (taskHubNameOverride != null)
            {
                eventSourcedSettings.HubName = taskHubNameOverride;
            }

            string runtimeLanguage = this.nameResolver.Resolve("FUNCTIONS_WORKER_RUNTIME");
            if (runtimeLanguage != null && !string.Equals(runtimeLanguage, "dotnet", StringComparison.OrdinalIgnoreCase))
            {
                eventSourcedSettings.CacheOrchestrationCursors = false; // cannot resume orchestrations in the middle
            }

            eventSourcedSettings.Validate((name) => this.nameResolver.Resolve(name));

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
