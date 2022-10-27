// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Logging.Abstractions;


    /// <summary>
    /// Resolves connection names by using a mapping from connection names to connection strings,
    /// or an already resolved connection string if specified inside the settings object.
    /// Used for compatibility with old configuration scheme.
    /// </summary>
    public class CompatibilityConnectionResolver : ConnectionNameToConnectionStringResolver
    {
        readonly NetheriteOrchestrationServiceSettings settings;

        /// <summary>
        /// Creates a connection string resolver for a given lookup function.
        /// </summary>
        /// <param name="connectionStringLookup">A function that maps connection names to connection strings.</param>
        public CompatibilityConnectionResolver(NetheriteOrchestrationServiceSettings settings, Func<string, string> connectionStringLookup)
            : base(connectionStringLookup)
        {
            this.settings = settings;
        }

#pragma warning disable CS0618 // Type or member is obsolete

        /// <inheritdoc/>
        public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType)
        {
            switch (recourceType)
            {
                case ResourceType.EventHubsNamespace:
                    if (!string.IsNullOrEmpty(this.settings.ResolvedTransportConnectionString))
                    {
                        return ConnectionInfo.FromEventHubsConnectionString(this.settings.ResolvedTransportConnectionString);
                    }
                    else
                    {
                        break;
                    }

                case ResourceType.BlobStorage:
                case ResourceType.TableStorage:
                    if (!string.IsNullOrEmpty(this.settings.ResolvedStorageConnectionString))
                    {
                        return ConnectionInfo.FromStorageConnectionString(this.settings.ResolvedStorageConnectionString, recourceType);
                    }
                    else
                    {
                        break;
                    }

                case ResourceType.PageBlobStorage:
                    var resolved = this.settings.ResolvedPageBlobStorageConnectionString ?? this.settings.ResolvedStorageConnectionString;
                    if (!string.IsNullOrEmpty(resolved))
                    {
                        return ConnectionInfo.FromStorageConnectionString(resolved, recourceType);
                    }
                    else
                    {
                        break;
                    }
            }

            return base.ResolveConnectionInfo(taskHub, connectionName, recourceType);
        }

        /// <inheritdoc/>
        public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
        {
            if (TransportConnectionString.IsPseudoConnectionString(this.settings.ResolvedTransportConnectionString))
            {
                TransportConnectionString.Parse(this.settings.ResolvedTransportConnectionString, out storageChoice, out transportChoice);
            }
            else
            {
                base.ResolveLayerConfiguration(connectionName, out storageChoice, out transportChoice);
            }
        }

#pragma warning restore CS0618 // Type or member is obsolete
    }
}

