// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Util
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text.RegularExpressions;

    // The code in this class is copied from the previous Azure Storage client SDKs,
    // which, unlike the newer SDKs, supported connection string parsing.
    static class ConnectionStringParser
    {
        public static void ParseStorageConnectionString(string connectionString, out string accountName, out Uri tableEndpoint, out Uri blobEndpoint)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException("connectionString");
            }

            // parse the string into individual property settings       

            IDictionary<string, string> connectionStringParameters = new Dictionary<string, string>();
            string[] propertySettings = connectionString.Split(new char[1] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < propertySettings.Length; i++)
            {
                string[] leftAndRightHandSide = propertySettings[i].Split(new char[1] { '=' }, 2);
                if (leftAndRightHandSide.Length != 2)
                {
                    throw new FormatException("Settings must be of the form \"name=value\".");
                }

                if (connectionStringParameters.ContainsKey(leftAndRightHandSide[0]))
                {
                    throw new FormatException(string.Format(CultureInfo.InvariantCulture, "Duplicate setting '{0}' found.", leftAndRightHandSide[0]));
                }

                connectionStringParameters.Add(leftAndRightHandSide[0], leftAndRightHandSide[1]);
            }

            if (connectionStringParameters.TryGetValue("UseDevelopmentStorage", out string useDevStorageString)
                && bool.TryParse(useDevStorageString, out bool useDevStorage)
                && useDevStorage)
            {
                accountName = "devstoreaccount1";

                UriBuilder uriBuilder;

                if (connectionStringParameters.TryGetValue("DevelopmentStorageProxyUri", out string proxyUriString))
                {
                    Uri proxyUri = new Uri(proxyUriString);
                    uriBuilder = new UriBuilder(proxyUri.Scheme, proxyUri.Host);
                }
                else
                {
                    uriBuilder = new UriBuilder("http", "127.0.0.1");
                }

                uriBuilder.Path = accountName;
                uriBuilder.Port = 10000;
                blobEndpoint = uriBuilder.Uri;
                uriBuilder.Port = 10002;
                tableEndpoint = uriBuilder.Uri;
            }
            else
            {
                // determine the account name

                if (!connectionStringParameters.TryGetValue("AccountName", out accountName))
                {
                    throw new FormatException("missing connection string parameter: AccountName");
                }

                // construct the service endpoints

                if (!connectionStringParameters.TryGetValue("DefaultEndpointsProtocol", out string scheme))
                {
                    throw new FormatException("missing connection string parameter: DefaultEndpointsProtocol");
                }

                if (!connectionStringParameters.TryGetValue("EndpointSuffix", out string endpointSuffix))
                {
                    endpointSuffix = "core.windows.net";
                }

                blobEndpoint = new Uri(string.Format(CultureInfo.InvariantCulture, "{0}://{1}.{2}.{3}/", scheme, accountName, "blob", endpointSuffix));
                tableEndpoint = new Uri(string.Format(CultureInfo.InvariantCulture, "{0}://{1}.{2}.{3}/", scheme, accountName, "table", endpointSuffix));
            }
        }
    }
}
