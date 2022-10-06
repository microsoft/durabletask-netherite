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

    public static class ConnectionInfoExtensions
    {
       

        public static async ValueTask<Microsoft.Azure.Storage.CloudStorageAccount> GetAzureStorageV11AccountAsync(this ConnectionInfo connectionInfo, CancellationToken cancellationToken)
        {
            if (connectionInfo.ConnectionString != null)
            {
                return Microsoft.Azure.Storage.CloudStorageAccount.Parse(connectionInfo.ConnectionString);
            }
            else
            {
                var credentials = new Microsoft.Azure.Storage.Auth.StorageCredentials(await connectionInfo.GetLegacyTokenCredentialAsync(cancellationToken));
                return new Microsoft.Azure.Storage.CloudStorageAccount(
                        storageCredentials: credentials,
                        accountName: connectionInfo.ResourceName,
                        endpointSuffix: connectionInfo.EndpointSuffix,
                        useHttps: true);
            }
        }

        public static Azure.Data.Tables.TableClient GetAzureStorageV12TableClientAsync(this ConnectionInfo connectionInfo, string tableName, CancellationToken cancellationToken)
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
                Uri uri = new Uri($"sb://{connectionInfo.FullyQualifiedResourceName}");
                var tokenProvider = new EventHubsTokenProvider(connectionInfo);
                return EventHubClient.CreateWithTokenProvider(uri, eventHub, tokenProvider);
            }
        }

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
                var storageAccount = await checkpointStorage.GetAzureStorageV11AccountAsync(CancellationToken.None);
                return new EventProcessorHost(
                      new Uri($"sb://{connectionInfo.FullyQualifiedResourceName}"),
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
                var bearerToken = (await connectionInfo.GetLegacyTokenCredentialAsync(cancellationToken)).Token;
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);
            }
        }
    }
}
