// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Core;
    using Microsoft.Extensions.Logging.Abstractions;

    /// <summary>
    /// Resolves connections using a token credential, a storage account name, and an eventhubs namespace name.
    /// </summary>
    public class SimpleCredentialResolver : ConnectionResolver
    {
        readonly Azure.Core.TokenCredential tokenCredential;
        readonly string storageAccountName;
        readonly string eventHubNamespaceName;

        /// <summary>
        /// Create a connection resolver that uses an Azure token credential.
        /// </summary>
        /// <param name="tokenCredential">The token credential to use.</param>
        /// <param name="storageAccountName">The name of the storage account, or null if using in-memory emulation.</param>
        /// <param name="eventHubNamespaceName">The name of the event hub namespace, or null if using the singlehost configuration.</param>
        public SimpleCredentialResolver(Azure.Core.TokenCredential tokenCredential, string storageAccountName = null, string eventHubNamespaceName = null)
        {
            this.tokenCredential = tokenCredential;
            this.storageAccountName = storageAccountName;
            this.eventHubNamespaceName = eventHubNamespaceName;
        }

        /// <inheritdoc/>
        public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType)
        {
            switch (recourceType)
            {
                case ResourceType.BlobStorage:
                case ResourceType.TableStorage:
                    return ConnectionInfo.FromTokenCredential(this.tokenCredential, this.storageAccountName, recourceType);

                case ResourceType.PageBlobStorage:
                    return null; // same as blob storage

                case ResourceType.EventHubsNamespace:
                    return ConnectionInfo.FromTokenCredential(this.tokenCredential, this.eventHubNamespaceName, recourceType);

                default:
                    throw new NotImplementedException("unknown resource type");
            }
        }

        /// <inheritdoc/>
        public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
        {
            if (string.IsNullOrEmpty(this.storageAccountName))
            {
                storageChoice = StorageChoices.Memory;
                transportChoice = TransportChoices.SingleHost;
            }
            else if (string.IsNullOrEmpty(this.eventHubNamespaceName))
            {
                storageChoice = StorageChoices.Faster;
                transportChoice = TransportChoices.SingleHost;
            }
            else
            {
                storageChoice = StorageChoices.Faster;
                transportChoice = TransportChoices.EventHubs;
            }
        }
    }
}