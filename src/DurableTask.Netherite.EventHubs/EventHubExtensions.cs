// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading.Tasks;
    using System.Threading;
    using Microsoft.Azure.EventHubs;
    using Azure.Core;
    using Microsoft.Azure.EventHubs.Processor;

    /// <summary>
    /// Utilities for constructing various SDK objects from a connection information.
    /// </summary>
    static class EventHubExtensions
    {
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

        class EventHubsTokenProvider : ITokenProvider
        {
            readonly ConnectionInfo info;

            public EventHubsTokenProvider(ConnectionInfo info)
            {
                this.info = info;
            }

            async Task<SecurityToken> ITokenProvider.GetTokenAsync(string appliesTo, TimeSpan timeout)
            {
                TokenRequestContext request = new(this.info.Scopes);
                AccessToken accessToken = await this.info.TokenCredential.GetTokenAsync(request, CancellationToken.None);
                return new JsonSecurityToken(accessToken.Token, appliesTo);
            }
        }
    }
}
