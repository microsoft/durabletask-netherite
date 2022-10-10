namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Logging.Abstractions;

    /// <summary>
    /// Resolves connection names by using a mapping from connection names to connection strings.
    /// </summary>
    public class ConnectionStringResolver : ConnectionResolver
    {
        readonly Func<string, string> connectionStringLookup;

        public ConnectionStringResolver(Func<string, string> connectionStringLookup)
        {
            this.connectionStringLookup = connectionStringLookup;
        }

        /// <inheritdoc/>
        public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType)
        {
            var connectionString = this.connectionStringLookup(connectionName);

            if (connectionString == null)
            {
                return null;
            }
            else if (recourceType == ResourceType.EventHubsNamespace)
            {
                return ConnectionInfo.FromEventHubsConnectionString(connectionString);
            }
            else if (recourceType == ResourceType.PageBlobStorage)
            {
                return null; // this resolver does not support using a separate page blob connection
            }
            else
            {
                return ConnectionInfo.FromStorageConnectionString(connectionString, recourceType);
            }
        }

        public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
        {
            if (TransportConnectionString.IsPseudoConnectionString(connectionName))
            {
                TransportConnectionString.Parse(connectionName, out storageChoice, out transportChoice);
            }
            else
            {
                var connectionString = this.connectionStringLookup(connectionName);

                if (TransportConnectionString.IsPseudoConnectionString(connectionName))
                {
                    TransportConnectionString.Parse(connectionName, out storageChoice, out transportChoice);
                }
                else
                {
                    // the default settings are Faster and EventHubs
                    storageChoice = StorageChoices.Faster;
                    transportChoice = TransportChoices.EventHubs;
                }
            }
        }
    }
}