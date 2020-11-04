// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

            // resolve any indirection in the specification of the two connection strings
            eventSourcedSettings.StorageConnectionString = this.ResolveIndirection(
                eventSourcedSettings.StorageConnectionString,
                nameof(NetheriteOrchestrationServiceSettings.StorageConnectionString));
            eventSourcedSettings.EventHubsConnectionString = this.ResolveIndirection(
                eventSourcedSettings.EventHubsConnectionString,
                nameof(NetheriteOrchestrationServiceSettings.EventHubsConnectionString));

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

            if (this.TraceToConsole || this.TraceToBlob)
            {
                // capture trace events generated in the backend and redirect them to additional sinks
                this.loggerFactory = new LoggerFactoryWrapper(this.loggerFactory, eventSourcedSettings.HubName, eventSourcedSettings.WorkerId, this);
            }


            eventSourcedSettings.HubName = this.extensionOptions.HubName;

            if (taskHubNameOverride != null)
            {
                eventSourcedSettings.HubName = taskHubNameOverride;
            }

            // TODO sanitize hubname

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
                    BlobLogger = new BlobLogger(settings.StorageConnectionString, settings.WorkerId);
                }

                var key = new DurableClientAttribute()
                {
                    TaskHub = settings.HubName,
                    ConnectionName = settings.StorageConnectionString,
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
                 string.Equals(this.defaultProvider.Settings.StorageConnectionString, settings.StorageConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return this.defaultProvider;
            }

            DurableClientAttribute key = new DurableClientAttribute()
            {
                TaskHub = settings.HubName,
                ConnectionName = settings.StorageConnectionString,
            };

            return this.cachedProviders.GetOrAdd(key, _ =>
            {
                //TODO support client-only version
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                return new NetheriteProvider(service, settings);
            });
        }

        string ResolveIndirection(string value, string propertyName)
        {
            string envName;
            string setting;

            if (string.IsNullOrEmpty(value))
            {
                envName = propertyName;
            }
            else if (value.StartsWith("$"))
            {
                envName = value.Substring(1);
            }
            else if (value.StartsWith("%") && value.EndsWith("%"))
            {
                envName = value.Substring(1, value.Length - 2);
            }
            else
            {
                envName = null;
            }

            if (envName != null)
            {
                setting = this.connectionStringResolver.Resolve(envName);
            }
            else
            {
                setting = value;
            }

            if (string.IsNullOrEmpty(setting))
            {
                throw new InvalidOperationException($"Could not resolve '{envName}' for required property '{propertyName}' in EventSourced storage provider settings.");
            }
            else
            {
                return setting;
            }
        }
    }
}
