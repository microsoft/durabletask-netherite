// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Net.Cache;
    using System.Net.Http;
    using System.Globalization;
    using System.Security.Cryptography;
    using System.Web;
    using DurableTask.Netherite.EventHubsTransport;
    using Microsoft.Azure.EventHubs;
    using Azure.Core;
    using System.Runtime.CompilerServices;
    using Microsoft.Azure.EventHubs.Processor;
    using Newtonsoft.Json.Serialization;
    using DurableTask.Netherite.Faster;

    /// <summary>
    /// Utilities for constructing various SDK objects from a connection information.
    /// </summary>
    public static class ConnectionInfoExtensions
    {
        /// <summary>
        /// Returns a classic (v11 SDK) storage account object.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <returns>A task for the storage account object.</returns>
        /// <exception cref="FormatException">Thrown if the host name of the connection info is not of the expected format {ResourceName}.{HostNameSuffix}.</exception>
        public static Task<Microsoft.Azure.Storage.CloudStorageAccount> GetAzureStorageV11AccountAsync(this ConnectionInfo connectionInfo)
        {
            // storage accounts run a token renewal timer, which we want to share for all instances
            if (connectionInfo.CachedStorageAccount == null)
            {
                connectionInfo.CachedStorageAccount = GetAsync();
            }
            return connectionInfo.CachedStorageAccount;

            async Task<Microsoft.Azure.Storage.CloudStorageAccount> GetAsync()
            {
                if (connectionInfo.ConnectionString != null)
                {
                    return Microsoft.Azure.Storage.CloudStorageAccount.Parse(connectionInfo.ConnectionString);
                }
                else
                {
                    var credentials = new Microsoft.Azure.Storage.Auth.StorageCredentials(await connectionInfo.ToLegacyCredentialAsync(CancellationToken.None));

                    string expectedHostNamePrefix = $"{connectionInfo.ResourceName}.";

                    if (!connectionInfo.HostName.StartsWith(expectedHostNamePrefix))
                    {
                        throw new FormatException("ConnectionInfo: unexpected format for host name");
                    }

                    return new Microsoft.Azure.Storage.CloudStorageAccount(
                            storageCredentials: credentials,
                            accountName: connectionInfo.ResourceName,
                            endpointSuffix: connectionInfo.HostName.Substring(expectedHostNamePrefix.Length),
                            useHttps: true);
                }
            }
        }

        /// <summary>
        /// Creates an Azure Storage table client for the v12 SDK.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns></returns>
        public static Azure.Data.Tables.TableClient GetAzureStorageV12TableClientAsync(this ConnectionInfo connectionInfo, string tableName, CancellationToken cancellationToken = default)
        {
            if (connectionInfo.ConnectionString != null)
            {
                return new Azure.Data.Tables.TableClient(connectionInfo.ConnectionString, tableName);
            }
            else
            {
                return new Azure.Data.Tables.TableClient(new Uri($"https://{connectionInfo.ResourceName}.table.core.windows.net/"), tableName, connectionInfo.TokenCredential);
            }
        }

        /// <summary>
        /// Creates an Event Hub client for the given connection info.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="eventHub">The event hub name.</param>
        /// <returns></returns>
        public static EventHubClient CreateEventHubClient(this ConnectionInfo connectionInfo, string eventHub)
        {
            if (connectionInfo.ConnectionString != null)
            {
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionInfo.ConnectionString)
                {
                    EntityPath = eventHub
                };
                return EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            }
            else
            {
                Uri uri = new Uri($"sb://{connectionInfo.HostName}");
                var tokenProvider = new EventHubsTokenProvider(connectionInfo);
                return EventHubClient.CreateWithTokenProvider(uri, eventHub, tokenProvider);
            }
        }

        /// <summary>
        /// Creates an event processor host for the given connection info.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="hostName">The host name.</param>
        /// <param name="eventHubPath">The event hub name.</param>
        /// <param name="consumerGroupName">The consumer group name.</param>
        /// <param name="checkpointStorage">A connection info for the checkpoint storage.</param>
        /// <param name="leaseContainerName">The name of the lease container.</param>
        /// <param name="storageBlobPrefix">A prefix for storing the blobs.</param>
        /// <returns>An event processor host.</returns>
        public static async Task<EventProcessorHost> GetEventProcessorHostAsync(
            this ConnectionInfo connectionInfo, 
            string hostName,
            string eventHubPath,
            string consumerGroupName,
            ConnectionInfo checkpointStorage,
            string leaseContainerName,
            string storageBlobPrefix)
        {
            if (connectionInfo.ConnectionString != null)
            {
                return new EventProcessorHost(
                       hostName,
                       eventHubPath,
                       consumerGroupName,
                       connectionInfo.ConnectionString,
                       checkpointStorage.ConnectionString,
                       leaseContainerName,
                       storageBlobPrefix);
            }
            else
            {
                var storageAccount = await checkpointStorage.GetAzureStorageV11AccountAsync();
                return new EventProcessorHost(
                      new Uri($"sb://{connectionInfo.HostName}"),
                      eventHubPath,
                      consumerGroupName,
                      (ITokenProvider) (new EventHubsTokenProvider(connectionInfo)),
                      storageAccount,
                      leaseContainerName,
                      storageBlobPrefix);
            }
        }

        class EventHubsTokenProvider : Microsoft.Azure.EventHubs.ITokenProvider
        {
            readonly ConnectionInfo info;

            public EventHubsTokenProvider(ConnectionInfo info)
            {
                this.info = info;
            }

            static TimeSpan NextRefresh(AccessToken token)
            {
                DateTimeOffset now = DateTimeOffset.UtcNow;
                return token.ExpiresOn - now - TimeSpan.FromMinutes(1); // refresh it a bit early.
            }

            async Task<SecurityToken> ITokenProvider.GetTokenAsync(string appliesTo, TimeSpan timeout)
            {
                TokenRequestContext request = new(this.info.Scopes);
                AccessToken accessToken = await this.info.TokenCredential.GetTokenAsync(request, CancellationToken.None);
                string audience = "???"; // Event Hubs SecurityToken requires this argument, but does not document what it means
                string tokentype = "???"; // Event Hubs  SecurityToken requires this argument, but does not document what it means
                return new SecurityToken(accessToken.Token, DateTime.UtcNow + NextRefresh(accessToken), audience, tokentype);
            }
        }

        /// <summary>
        /// Adds the necessary authorization headers to a REST http request.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="request">The request object.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns></returns>
        public static async Task AuthorizeHttpRequestMessage(this ConnectionInfo connectionInfo, HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (connectionInfo.ConnectionString != null)
            {
                // parse the eventhubs connection string to extract various parameters
                var properties = Azure.Messaging.EventHubs.EventHubsConnectionStringProperties.Parse(connectionInfo.ConnectionString);
                string resourceUri = properties.Endpoint.AbsoluteUri;
                string keyName = properties.SharedAccessKeyName; 
                string key = properties.SharedAccessKey;

                // create a token (from https://docs.microsoft.com/en-us/rest/api/eventhub/generate-sas-token#c)
                TimeSpan sinceEpoch = DateTime.UtcNow - new DateTime(1970, 1, 1);
                var week = 60 * 60 * 24 * 7;
                var expiry = Convert.ToString((int)sinceEpoch.TotalSeconds + week);
                string stringToSign = HttpUtility.UrlEncode(resourceUri) + "\n" + expiry;
                HMACSHA256 hmac = new HMACSHA256(Encoding.UTF8.GetBytes(key));
                var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
                var sasToken = String.Format(CultureInfo.InvariantCulture, "SharedAccessSignature sr={0}&sig={1}&se={2}&skn={3}", HttpUtility.UrlEncode(resourceUri), HttpUtility.UrlEncode(signature), expiry, keyName);
               
                //add it to the request
                request.Headers.Add("Authorization", sasToken);
            }
            else
            {
                var bearerToken = (await connectionInfo.ToLegacyCredentialAsync(cancellationToken)).Token;
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);
            }
        }
    }
}
