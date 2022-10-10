// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Can resolve named connections into ConnectionInfo objects that contain all the parameters needed to connect.
    /// </summary>
    public abstract class ConnectionResolver
    {
        public enum ResourceType
        {
            EventHubsNamespace,
            BlobStorage,
            TableStorage,
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
        /// Resolves the layers to use, such as for emulation or single host configurations.
        /// </summary>
        /// <param name="connectionName">The connection name.</param>
        /// <param name="storageChoice">The storage layer to use.</param>
        /// <param name="transportChoice">The transport layer to use.</param>
        public abstract void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice);
    }
}
