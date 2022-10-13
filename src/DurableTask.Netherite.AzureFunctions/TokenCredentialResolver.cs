// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Identity;
    using Microsoft.Extensions.Logging.Abstractions;

#if !NETCOREAPP2_2

    /// <summary>
    /// Resolves connections using a token credential and a mapping from connection names to resource names.
    /// </summary>
    public abstract class CredentialBasedConnectionNameResolver : DurableTask.Netherite.ConnectionResolver
    {
        readonly Azure.Core.TokenCredential tokenCredential;

        /// <summary>
        /// Create a connection resolver that uses an Azure token credential.
        /// </summary>
        /// <param name="tokenCredential">The token credential to use.</param>
        public CredentialBasedConnectionNameResolver(Azure.Core.TokenCredential tokenCredential)
        {      
            this.tokenCredential = tokenCredential;
        }

        public abstract string GetStorageAccountName(string connectionName);

        public abstract string GetEventHubsNamespaceName(string connectionName);

        /// <inheritdoc/>
        public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType)
        {
            switch (recourceType)
            {
                case ResourceType.BlobStorage:
                case ResourceType.TableStorage:
                    string storageAccountName = this.GetStorageAccountName(connectionName);
                    if (string.IsNullOrEmpty(storageAccountName))
                    {
                        throw new ArgumentException("GetStorageAccountName returned invalid result");
                    }
                    return ConnectionInfo.FromTokenCredential(this.tokenCredential, storageAccountName, recourceType);

                case ResourceType.PageBlobStorage:
                    return null; // same as blob storage

                case ResourceType.EventHubsNamespace:
                    string eventHubsNamespaceName = this.GetEventHubsNamespaceName(connectionName);
                    if (string.IsNullOrEmpty(eventHubsNamespaceName))
                    {
                        throw new ArgumentException("GetEventHubsNamespaceName returned invalid result");
                    }
                    return ConnectionInfo.FromTokenCredential(this.tokenCredential, eventHubsNamespaceName, recourceType);

                default:
                    throw new NotImplementedException("unknown resource type");
            }   
        }

        /// <inheritdoc/>
        public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
        {
            if (TransportConnectionString.IsPseudoConnectionString(connectionName))
            {
                TransportConnectionString.Parse(connectionName, out storageChoice, out transportChoice);
            }
            else
            {
                storageChoice = StorageChoices.Faster;
                transportChoice = TransportChoices.EventHubs;
            }
        }
    }

#endif
}