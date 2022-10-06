// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Connections;
    using Microsoft.Azure.Cosmos.Table;
    using Microsoft.Azure.Storage;
    using Microsoft.Identity.Client;
    using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;

    /// <summary>
    /// Internal abstraction used for capturing connection information and credentials.
    /// Represents all different kinds of storage (blobs, tables, event hubs namespaces).
    /// </summary>
    public class ConnectionInfo
    {
        /// <summary>
        ///  The name of the resource.
        /// </summary>
        public string ResourceName { get; set; }

        /// <summary>
        /// A connection string for accessing this resource.
        /// (may be null if using a <see cref="TokenCredential"/>).
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// A token credential for accessing this resource.
        /// (may be null if using a <see cref="ConnectionString"/>).
        /// </summary>
        public Azure.Core.TokenCredential TokenCredential { get; set; }

        /// <summary>
        /// The endpoint suffix.
        /// (may be null if using a <see cref="ConnectionString"/>).
        /// </summary>
        public string EndpointSuffix { get; set; }

        /// <summary>
        /// The fully qualified name for the resource. 
        /// </summary>
        public string FullyQualifiedResourceName => $"{this.ResourceName}.{this.EndpointSuffix}";

        /// <summary>
        /// Scopes for the token.
        /// </summary>
        public string[] Scopes;

        /// <summary>
        /// Older versions of the storage SDK require a different kind of TokenCredential.
        /// Call this to automatically convert the token credential. 
        /// Since the conversion shim runs a renewal timer, we cache it.
        /// </summary>
        /// <returns></returns>
        public async ValueTask<Microsoft.Azure.Storage.Auth.TokenCredential> GetLegacyTokenCredentialAsync(CancellationToken cancellationToken)
        {
            if (this.legacyTokenCredential == null)
            {
                this.legacyTokenCredential = await this.ToLegacyCredential(cancellationToken);
            }
            return this.legacyTokenCredential;
        }
        Microsoft.Azure.Storage.Auth.TokenCredential legacyTokenCredential;
    }
}
