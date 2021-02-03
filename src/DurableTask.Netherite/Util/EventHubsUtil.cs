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
        public static async Task<bool> EnsureEventHubExistsAsync(string connectionString, string eventHubName, int partitionCount)
        {
            // parse the eventhubs connection string to extract various parameters
            var properties = Azure.Messaging.EventHubs.EventHubsConnectionStringProperties.Parse(connectionString);
            string resource = properties.FullyQualifiedNamespace;
            string resourceUri = properties.Endpoint.AbsoluteUri;
            string name = properties.SharedAccessKeyName;
            string key = properties.SharedAccessKey;

            // create a token that allows us to create an eventhub
            string sasToken = createToken(resourceUri, name, key);

            // the following token creation code is taken from https://docs.microsoft.com/en-us/rest/api/eventhub/generate-sas-token#c
            string createToken(string resourceUri, string name, string key)
            {
                TimeSpan sinceEpoch = DateTime.UtcNow - new DateTime(1970, 1, 1);
                var week = 60 * 60 * 24 * 7;
                var expiry = Convert.ToString((int)sinceEpoch.TotalSeconds + week);
                string stringToSign = HttpUtility.UrlEncode(resourceUri) + "\n" + expiry;
                HMACSHA256 hmac = new HMACSHA256(Encoding.UTF8.GetBytes(key));
                var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
                var sasToken = String.Format(CultureInfo.InvariantCulture, "SharedAccessSignature sr={0}&sig={1}&se={2}&skn={3}", HttpUtility.UrlEncode(resourceUri), HttpUtility.UrlEncode(signature), expiry, name);
                return sasToken;
            }

            // send an http request to create the eventhub
            HttpClient client = new HttpClient();
            var request = new HttpRequestMessage();
            request.RequestUri = new Uri($"https://{resource}/{eventHubName}?timeout=60&api-version=2014-01");
            request.Method = HttpMethod.Put;
            request.Content = new StringContent(@" 
                            <entry xmlns='http://www.w3.org/2005/Atom'>  
                              <content type='application/xml'>  
                                <EventHubDescription xmlns:i='http://www.w3.org/2001/XMLSchema-instance' xmlns='http://schemas.microsoft.com/netservices/2010/10/servicebus/connect'>  
                                  <MessageRetentionInDays>1</MessageRetentionInDays>  
                                  <PartitionCount>" + partitionCount + @"</PartitionCount>  
                                </EventHubDescription>  
                              </content>  
                            </entry>",
                            Encoding.UTF8,
                            "application/xml");
            request.Headers.Add("Authorization", sasToken);
            request.Headers.Add("Host", resource);

            var response = await client.SendAsync(request);

            if (response.StatusCode != System.Net.HttpStatusCode.Conflict)
            {
                response.EnsureSuccessStatusCode();
            }

            return response.StatusCode == System.Net.HttpStatusCode.Created;         
        }
    }
}
