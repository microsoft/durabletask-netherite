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
    using Azure.Core;
    using System.Runtime.CompilerServices;
    using Newtonsoft.Json.Serialization;
    using DurableTask.Netherite.Faster;
    using Azure.Storage.Blobs;
    using Azure.Messaging.EventHubs;

    /// <summary>
    /// Utilities for constructing various SDK objects from a connection information.
    /// </summary>
    static class ConnectionInfoExtensions
    {
        /// <summary>
        /// Returns a classic (v11 SDK) storage account object.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <returns>A task for the storage account object.</returns>
        /// <exception cref="FormatException">Thrown if the host name of the connection info is not of the expected format {ResourceName}.{HostNameSuffix}.</exception>
        public static Task<Microsoft.Azure.Storage.CloudStorageAccount> GetAzureStorageV11AccountAsync(this ConnectionInfo connectionInfo)
        {
            // storage accounts run a token renewal timer, so we want to share a single instance
            if (connectionInfo.CachedStorageAccountTask == null)
            {
                connectionInfo.CachedStorageAccountTask = GetAsync();
            }
            return connectionInfo.CachedStorageAccountTask;

            async Task<Microsoft.Azure.Storage.CloudStorageAccount> GetAsync()
            {
                if (connectionInfo.ConnectionString != null)
                {
                    return Microsoft.Azure.Storage.CloudStorageAccount.Parse(connectionInfo.ConnectionString);
                }
                else
                {
                    var credentials = new Microsoft.Azure.Storage.Auth.StorageCredentials(await connectionInfo.ToLegacyCredentialAsync(CancellationToken.None));

                    // hostnames are generally structured like
                    //    accountname.blob.core.windows.net
                    //    accountname.table.core.windows.net
                    //    databasename.table.cosmos.azure.com

                    int firstDot = connectionInfo.HostName.IndexOf('.');
                    int secondDot = connectionInfo.HostName.IndexOf('.', firstDot + 1);
                    string hostNameSuffix = connectionInfo.HostName.Substring(secondDot + 1);

                    return new Microsoft.Azure.Storage.CloudStorageAccount(
                            storageCredentials: credentials,
                            accountName: connectionInfo.ResourceName,
                            endpointSuffix: hostNameSuffix,
                            useHttps: true);
                }
            }
        }

        /// <summary>
        /// Creates an Azure Storage table client for the v12 SDK.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns></returns>
        public static Azure.Data.Tables.TableClient GetAzureStorageV12TableClient(this ConnectionInfo connectionInfo, string tableName)
        {
            if (connectionInfo.ConnectionString != null)
            {
                return new Azure.Data.Tables.TableClient(connectionInfo.ConnectionString, tableName);
            }
            else
            {
                return new Azure.Data.Tables.TableClient(new Uri($"https://{connectionInfo.HostName}/"), tableName, connectionInfo.TokenCredential);
            }
        }

        /// <summary>
        /// Creates an Azure Storage blob client for the v12 SDK.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="blobClientOptions">The blob client options.</param>
        /// <returns></returns>
        public static Azure.Storage.Blobs.BlobServiceClient GetAzureStorageV12BlobServiceClient(this ConnectionInfo connectionInfo, BlobClientOptions blobClientOptions)
        {
            if (connectionInfo.ConnectionString != null)
            {
                return new Azure.Storage.Blobs.BlobServiceClient(connectionInfo.ConnectionString, blobClientOptions);
            }
            else
            {
                return new Azure.Storage.Blobs.BlobServiceClient(new Uri($"https://{connectionInfo.HostName}/"), connectionInfo.TokenCredential, blobClientOptions);
            }
        }

        /// <summary>
        /// Creates an Event Hub connection.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="eventHub">The event hub name.</param>
        /// <returns></returns>
        public static EventHubConnection CreateEventHubConnection(this ConnectionInfo connectionInfo, string eventHub)
        {
            if (connectionInfo.ConnectionString != null)
            {
                return new Azure.Messaging.EventHubs.EventHubConnection(connectionInfo.ConnectionString, eventHub);
            }
            else
            {
                return new Azure.Messaging.EventHubs.EventHubConnection(
                    fullyQualifiedNamespace: connectionInfo.HostName,
                    eventHubName: eventHub,
                    credential: connectionInfo.TokenCredential,
                    connectionOptions: null);
            }
        }

        /// <summary>
        /// Creates an event processor host for the given connection info.
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="args">The constructor arguments.</param>
        /// <returns>An event processor host.</returns>
        public static EventProcessorHost CreateEventProcessorHost(
            this ConnectionInfo connectionInfo,
            EventProcessorHost.ConstructorArguments args)
        {       
            if (connectionInfo.ConnectionString != null)
            {
                return new EventProcessorHost(args, connectionInfo.ConnectionString);
            }
            else
            {
                return new EventProcessorHost(args, connectionInfo.HostName, connectionInfo.TokenCredential);
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
