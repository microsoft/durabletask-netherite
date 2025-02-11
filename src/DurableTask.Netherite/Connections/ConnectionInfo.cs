// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using DurableTask.Netherite.Util;

    /// <summary>
    /// Internal abstraction used for capturing connection information and credentials.
    /// Represents all different kinds of storage (blobs, tables, event hubs namespaces).
    /// </summary>
    public partial class ConnectionInfo
    {
        /// <summary>
        ///  The name of the resource.
        /// </summary>
        public string ResourceName { get; set; }

        /// <summary>
        /// A connection string for accessing this resource.
        /// (may be null if using a <see cref="TokenCredential"/>).
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// A token credential for accessing this resource.
        /// (may be null if using a <see cref="ConnectionString"/>).
        /// </summary>
        public Azure.Core.TokenCredential TokenCredential { get; set; }

        /// <summary>
        /// The fully qualified name for the resource. 
        /// </summary>
        public string HostName { get; set; }

        /// <summary>
        /// Scopes for the token.
        /// </summary>
        public string[] Scopes;

        protected static readonly string[] s_storage_scopes = { "https://storage.azure.com/.default" };
        protected static readonly string[] s_eventhubs_scopes = { "https://eventhubs.azure.net/.default" };

        /// <summary>
        /// Creates a connection info from an event hubs connection string
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>The connection info.</returns>
        public static ConnectionInfo FromEventHubsConnectionString(string connectionString)
        {
            var properties = Azure.Messaging.EventHubs.EventHubsConnectionStringProperties.Parse(connectionString);
            string hostName = properties.FullyQualifiedNamespace;
            string nameSpaceName = hostName.Split('.')[0];

            return new ConnectionInfo()
            {
                ResourceName = nameSpaceName,
                ConnectionString = connectionString,
                TokenCredential = null,
                HostName = hostName,
                Scopes = s_eventhubs_scopes,
            };
        }

        /// <summary>
        /// Creates a connection info from a storage connection string
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="resourceType">The resource type being accessed.</param>
        /// <returns>The connection info.</returns>
        public static ConnectionInfo FromStorageConnectionString(string connectionString, ConnectionResolver.ResourceType resourceType)
        {
            ConnectionStringParser.ParseStorageConnectionString(connectionString, out string accountName, out Uri tableEndpoint, out Uri blobEndpoint);
            
            return new ConnectionInfo()
            {
                ResourceName = accountName,
                ConnectionString = connectionString,
                TokenCredential = null,
                HostName = GetEndpoint().Host,
                Scopes = s_storage_scopes,
            };

            Uri GetEndpoint() => resourceType == ConnectionResolver.ResourceType.TableStorage 
                ? tableEndpoint : blobEndpoint;
        }

        /// <summary>
        /// Creates a connection info from a token credential.
        /// </summary>
        /// <param name="tokenCredential">The token credential.</param>
        /// <param name="name">The name of the resource (account name or namespace name).</param>
        /// <param name="resourceType">The type of the resource.</param>
        /// <returns></returns>
        /// <returns>The connection info.</returns>
        public static ConnectionInfo FromTokenCredential(Azure.Core.TokenCredential tokenCredential, string name, ConnectionResolver.ResourceType resourceType)
        {
            switch (resourceType)
            {
                case ConnectionResolver.ResourceType.EventHubsNamespace:
                    {
                        return new ConnectionInfo()
                        {
                            ResourceName = name,
                            ConnectionString = null,
                            TokenCredential = tokenCredential,
                            HostName = $"{name}.servicebus.windows.net",
                            Scopes = s_eventhubs_scopes,
                        };
                    }

                case ConnectionResolver.ResourceType.BlobStorage:
                case ConnectionResolver.ResourceType.PageBlobStorage:
                    return new ConnectionInfo()
                    {
                        ResourceName = name,
                        ConnectionString = null,
                        TokenCredential = tokenCredential,
                        HostName = $"{name}.blob.core.windows.net",
                        Scopes = s_storage_scopes,
                    };


                case ConnectionResolver.ResourceType.TableStorage:
                    {
                        return new ConnectionInfo()
                        {
                            ResourceName = name,
                            ConnectionString = null,
                            TokenCredential = tokenCredential,
                            HostName = $"{name}.table.core.windows.net",
                            Scopes = s_storage_scopes,
                        };
                    }

                default:
                    return null;
            }
        }

        /// <summary>
        /// Creates a connection info from a token credential and a endpoint.
        /// </summary>
        /// <param name="tokenCredential">The token credential.</param>
        /// <param name="host">The name of the host (which must always start with the resource name).</param>
        /// <param name="resourceType">The type of the resource.</param>
        /// <returns></returns>
        /// <returns>The connection info.</returns>
        public static ConnectionInfo FromTokenCredentialAndHost(Azure.Core.TokenCredential tokenCredential, string host, ConnectionResolver.ResourceType resourceType)
        {
            return new ConnectionInfo()
            {
                ResourceName = host.Split('.')[0],
                ConnectionString = null,
                TokenCredential = tokenCredential,
                HostName = host,
                Scopes = resourceType == ConnectionResolver.ResourceType.EventHubsNamespace ? s_eventhubs_scopes : s_storage_scopes,
            };
        }

        /// <summary>
        /// Get an access token for the resource.
        /// </summary>
        /// <param name="resourceType"></param>
        /// <returns></returns>
        public ValueTask<AccessToken> GetTokenAsync(ConnectionResolver.ResourceType resourceType, CancellationToken cancellation)
        {
            if (this.TokenCredential == null)
            {
                throw new InvalidOperationException("missing token credential");
            }

            TokenRequestContext requestContext = new(resourceType == ConnectionResolver.ResourceType.EventHubsNamespace ? s_eventhubs_scopes : s_storage_scopes);

            return this.TokenCredential.GetTokenAsync(requestContext, cancellation);
        }
    }
}
