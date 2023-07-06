// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Globalization;
    using System.Net.Http;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web;

    /// <summary>
    /// Utility functions for dealing with event hubs.
    /// </summary>
    public static class EventHubsUtil
    {
        /// <summary>
        /// Ensures a particular event hub exists, creating it if necessary.
        /// </summary>
        /// <param name="connectionString">The SAS connection string for the namespace.</param>
        /// <param name="eventHubName">The name of the event hub.</param>
        /// <param name="partitionCount">The number of partitions to create, if the event hub does not already exist.</param>
        /// <returns>true if the event hub was created.</returns>
        public static async Task<bool> EnsureEventHubExistsAsync(ConnectionInfo info, string eventHubName, int partitionCount, CancellationToken cancellationToken)
        {
            var response = await SendEventHubRequest(info, eventHubName, partitionCount, cancellationToken);
            if (response.StatusCode != System.Net.HttpStatusCode.Conflict)
            {
                response.EnsureSuccessStatusCode();
            }
            return response.StatusCode == System.Net.HttpStatusCode.Created;
        }

        /// <summary>
        /// Deletes a particular event hub if it exists.
        /// </summary>
        /// <param name="connectionString">The SAS connection string for the namespace.</param>
        /// <param name="eventHubName">The name of the event hub.</param>
        /// <returns>true if the event hub was deleted.</returns>
        public static async Task<bool> DeleteEventHubIfExistsAsync(ConnectionInfo info, string eventHubName, CancellationToken cancellationToken)
        {
            var response = await SendEventHubRequest(info, eventHubName, null, cancellationToken);
            if (response.StatusCode != System.Net.HttpStatusCode.NotFound)
            {
                response.EnsureSuccessStatusCode();
            }
            return response.StatusCode == System.Net.HttpStatusCode.OK;
        }

        public static async Task<bool> EnsureConsumerGroupExistsAsync(ConnectionInfo info, string eventHubName, string consumerGroup, CancellationToken cancellationToken)
        {
            var response = await SendConsumerGroupRequest(info, eventHubName, consumerGroup, delete: false, cancellationToken);
            if (response.StatusCode != System.Net.HttpStatusCode.Conflict)
            {
                response.EnsureSuccessStatusCode();
            }
            return response.StatusCode == System.Net.HttpStatusCode.Created;
        }

        // for documentation of these APIs, see https://learn.microsoft.com/en-us/rest/api/eventhub/event-hubs-management-rest

        static async Task<HttpResponseMessage> SendEventHubRequest(ConnectionInfo info, string eventHubPath, int? partitionCount, CancellationToken cancellationToken)
        {
            // send an http request to create or delete the eventhub
            HttpClient client = new HttpClient();
            var request = new HttpRequestMessage();
            request.RequestUri = new Uri($"https://{info.HostName}/{eventHubPath}?timeout=60&api-version=2014-01");
            request.Method = partitionCount.HasValue ? HttpMethod.Put : HttpMethod.Delete;
            if (partitionCount.HasValue)
            {
                request.Content = new StringContent(@" 
                            <entry xmlns='http://www.w3.org/2005/Atom'>  
                              <content type='application/xml'>  
                                <EventHubDescription xmlns:i='http://www.w3.org/2001/XMLSchema-instance' xmlns='http://schemas.microsoft.com/netservices/2010/10/servicebus/connect'>  
                                  <MessageRetentionInDays>1</MessageRetentionInDays>  
                                  <PartitionCount>" + partitionCount.Value + @"</PartitionCount>  
                                </EventHubDescription>  
                              </content>  
                            </entry>",
                                Encoding.UTF8,
                                "application/xml");
            }
            request.Headers.Add("Host", info.HostName);

            // add an authorization header to the request
            await info.AuthorizeHttpRequestMessage(request, cancellationToken);

            return await client.SendAsync(request);
        }

        static async Task<HttpResponseMessage> SendConsumerGroupRequest(ConnectionInfo info, string eventHubPath, string consumerGroupName, bool delete, CancellationToken cancellationToken)
        {
            // send an http request to create or delete the eventhub
            HttpClient client = new HttpClient();
            var request = new HttpRequestMessage();
            request.RequestUri = new Uri($"https://{info.HostName}/{eventHubPath}/consumerGroups/{consumerGroupName}?timeout=60&api-version=2014-01");
            request.Method = delete ? HttpMethod.Delete : HttpMethod.Put;
            request.Content = new StringContent(@" 
                            <entry xmlns='http://www.w3.org/2005/Atom'>  
                              <content type='application/xml'>  
                                <ConsumerGroupDescription xmlns:i='http://www.w3.org/2001/XMLSchema-instance' xmlns='http://schemas.microsoft.com/netservices/2010/10/servicebus/connect'>
                                </ConsumerGroupDescription>
                              </content>  
                            </entry>",
                               Encoding.UTF8,
                               "application/xml");

            request.Headers.Add("Host", info.HostName);

            // add an authorization header to the request
            await info.AuthorizeHttpRequestMessage(request, cancellationToken);

            return await client.SendAsync(request);
        }
    }
}
