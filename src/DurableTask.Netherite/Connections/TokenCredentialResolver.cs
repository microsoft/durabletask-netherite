namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Identity;
    using Microsoft.Extensions.Logging.Abstractions;

    /// <summary>
    /// Resolves connection names using a token credential and a name-to-resource mapping.
    /// </summary>
    public class TokenCredentialResolver : ConnectionResolver
    {
        readonly Azure.Core.TokenCredential credential;
        readonly Func<string, string> nameResolver;

        /// <summary>
        /// Create a connection resolver based on a token credential and a function that maps connection names to resource names.
        /// </summary>
        /// <param name="tokenCredential">The token credential to use.</param>
        /// <param name="nameResolver">A mapping from connection names to resource names.</param>
        public TokenCredentialResolver(Azure.Core.TokenCredential tokenCredential, Func<string, string> nameResolver)
        {
            this.nameResolver = nameResolver;
            this.credential = tokenCredential;
        }

        /// <inheritdoc/>
        public override ConnectionInfo ResolveConnectionInfo(string taskHub, string connectionName, ResourceType recourceType)
        {
            string resourceName = this.nameResolver(connectionName);
            return ConnectionInfo.FromTokenCredential(this.credential, resourceName, recourceType);
        }

        public override void ResolveLayerConfiguration(string connectionName, out StorageChoices storageChoice, out TransportChoices transportChoice)
        {
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