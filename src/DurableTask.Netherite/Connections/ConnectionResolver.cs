// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// An abstract class that represents a method for resolving named connections into ConnectionInfo objects, which can then be used to connect.
    /// </summary>
    public abstract class ConnectionResolver
    {
        /// <summary>
        /// The cloud resources for which Netherite may require connection information
        /// </summary>
        public enum ResourceType
        {
            /// <summary>
            /// The event hubs namespace. Used for connecting various logical components (partitions, clients, and the load monitor).
            /// Not required if the layer configuration uses <see cref="TransportChoices.SingleHost"/>.
            /// </summary>
            EventHubsNamespace,

            /// <summary>
            /// The blob storage account, used for task hub storage and event hub consumer checkpoints. 
            /// Not required if the layer configuration uses <see cref="StorageChoices.Memory"/>.
            /// </summary>
            BlobStorage,

            /// <summary>
            /// The table storage account, used for publishing load information. 
            /// Not required if <see cref="NetheriteOrchestrationServiceSettings.LoadInformationAzureTableName"/> is set to null, or if
            /// the layer configuration uses <see cref="StorageChoices.Memory"/>.
            /// </summary>
            TableStorage,

            /// <summary>
            /// The page blob storage account. Optional, can be used for storing page blobs separately from other blobs. 
            /// </summary>
            PageBlobStorage,
        }

        /// <summary>
        /// Attempts to resolves the given name to obtain connection information.
        /// </summary>
        /// <param name="taskHub">The name of the task hub.</param>
        /// <param name="connectionName">The connection name.</param>
        /// <param name="recourceType">The type of resource to which a connection is desired to be made.</param>
        /// <returns>A ConnectionInfo with the required parameters to connect, or null if not found.</returns>
        public abstract ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType);

        /// <summary>
        /// Determines the layers to use. For example, can configure full emulation by assigning <see cref="StorageChoices.Memory"/> and <see cref="TransportChoices.SingleHost"/>.
        /// </summary>
        /// <param name="connectionName">The connection name.</param>
        /// <param name="storageChoice">The storage layer to use.</param>
        /// <param name="transportChoice">The transport layer to use.</param>
        public abstract void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice);

        /// <summary>
        /// Creates a connection resolver for a given connection string lookup function.
        /// </summary>
        /// <param name="connectionStringLookup">A function that maps connection names to connection strings.</param>
        public static ConnectionResolver FromConnectionNameToConnectionStringResolver(Func<string, string> connectionStringLookup)
        {
            return new ConnectionNameToConnectionStringResolver(connectionStringLookup);
        }

        /// <summary>
        /// Creates a connection resolver given a token credential, a storage account name, and an event hubs namespace name.
        /// Applies the default suffixes (*.core.windows.net for storage accounts and *.servicebus.windows.net for event hubs).
        /// </summary>
        /// <param name="tokenCredential">The token credential to use.</param>
        /// <param name="storageAccountName">The name of the storage account, or null if using in-memory emulation.</param>
        /// <param name="eventHubNamespaceName">The name of the event hub namespace, or null if using the singlehost configuration.</param>   
        public static ConnectionResolver FromTokenCredentialAndResourceNames(Azure.Core.TokenCredential tokenCredential, string storageAccountName = null, string eventHubNamespaceName = null)
        {
            return new SimpleCredentialResolver(tokenCredential, storageAccountName, eventHubNamespaceName);
        }
    }
}
