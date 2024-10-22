namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Security.Cryptography;
    using System.Text;
    using Azure.Identity;
    using Azure.Messaging.EventHubs;
    using DurableTask.Netherite;
    using Microsoft.Extensions.Azure;
    using Microsoft.Extensions.Configuration;

    /// <summary>
    /// Resolves connections using an AzureComponentFactory and configuration sections.
    /// </summary>
    public class ConfigurationSectionBasedConnectionNameResolver : DurableTask.Netherite.ConnectionResolver
    {
        readonly AzureComponentFactory componentFactory;
        readonly IConfiguration configuration;

        public ConfigurationSectionBasedConnectionNameResolver(AzureComponentFactory componentFactory, IConfiguration configuration)
        {
            this.componentFactory = componentFactory;
            this.configuration = configuration;
        }

        public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
        {
            if (TransportConnectionString.IsPseudoConnectionString(connectionName))
            {
                TransportConnectionString.Parse(connectionName, out storageChoice, out transportChoice);
            }
            else
            {
                IConfigurationSection connectionSection = this.configuration.GetWebJobsConnectionStringSection(connectionName);

                if (TransportConnectionString.IsPseudoConnectionString(connectionSection.Value))
                {
                    TransportConnectionString.Parse(connectionSection.Value, out storageChoice, out transportChoice);
                }
                else
                {
                    // the default settings are Faster and EventHubs
                    storageChoice = StorageChoices.Faster;
                    transportChoice = TransportChoices.EventHubs;
                }
            }
        }

        public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType resourceType)
        {
            switch (resourceType)
            {
                case ResourceType.BlobStorage:
                case ResourceType.TableStorage:
                case ResourceType.PageBlobStorage:
                    return this.ResolveStorageAccountConnection(connectionName, resourceType);

                case ResourceType.EventHubsNamespace:
                    return this.ResolveEventHubsConnection(connectionName);

                default:
                    throw new NotSupportedException("unknown resource type");
            }
        }

        public ConnectionInfo ResolveStorageAccountConnection(string connectionName, ResourceType resourceType)
        {
            IConfigurationSection connectionSection = this.configuration.GetWebJobsConnectionStringSection(connectionName);

            if (!string.IsNullOrWhiteSpace(connectionSection.Value))
            {
                // It's a connection string
                return ConnectionInfo.FromStorageConnectionString(connectionSection.Value, resourceType);
            }

            // parse some of the relevant fields in the configuration section
            StorageAccountOptions accountOptions = connectionSection.Get<StorageAccountOptions>();

            var tokenCredential = this.componentFactory.CreateTokenCredential(connectionSection);

            return ConnectionInfo.FromTokenCredentialAndHost(tokenCredential, accountOptions.GetHost(resourceType), resourceType);
        }

        class StorageAccountOptions
        {
            public string AccountName { get; set; }

            public Uri BlobServiceUri { get; set; }

            public Uri TableServiceUri { get; set; }

            public string GetHost(ResourceType resourceType)
            {
                switch (resourceType)
                {
                    case ResourceType.BlobStorage:
                    case ResourceType.PageBlobStorage:
                        return this.BlobServiceUri?.Host ?? $"{this.AccountName}.blob.core.windows.net";

                    case ResourceType.TableStorage:
                        return this.TableServiceUri?.Host ?? $"{this.AccountName}.table.core.windows.net";

                    default:
                        throw new NotSupportedException("unknown resource type");
                }
            }
        }

        public ConnectionInfo ResolveEventHubsConnection(string connectionName)
        {
            IConfigurationSection connectionSection = this.configuration.GetWebJobsConnectionStringSection(connectionName);
            if (!connectionSection.Exists())
            {
                // A common mistake is for developers to set their `connection` to a full connection string rather
                // than an informational name. We handle this case specifically, to be helpful, and to avoid leaking secrets in error messages.
                try
                {
                    var properties = EventHubsConnectionStringProperties.Parse(connectionName);

                    // we parsed without exception, so it's a connection string.
                    // We now throw a descriptive and secret-free exception.

                    throw new NetheriteConfigurationException($"a full event hubs connection string was incorrectly used instead of a connection setting name");
                }
                catch (FormatException)
                {
                }
                
                // Not found
                throw new NetheriteConfigurationException($"EventHub account connection string with name '{connectionName}' does not exist in the settings. " +
                                                          $"Make sure that it is a defined App Setting.");
            }

            if (!string.IsNullOrWhiteSpace(connectionSection.Value))
            {
                // It's a connection string
                return ConnectionInfo.FromEventHubsConnectionString(connectionSection.Value);
            }

            var fullyQualifiedNamespace = connectionSection["fullyQualifiedNamespace"];
            if (string.IsNullOrWhiteSpace(fullyQualifiedNamespace))
            {
                // We could not find the necessary parameter
                throw new NetheriteConfigurationException($"Configuration for event hubs connection should have a 'fullyQualifiedNamespace' property or be a string representing a connection string.");
            }

            var tokenCredential = this.componentFactory.CreateTokenCredential(connectionSection);

            return ConnectionInfo.FromTokenCredentialAndHost(tokenCredential, fullyQualifiedNamespace, ResourceType.EventHubsNamespace);
        }
    }
}
