// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Core;
    using Microsoft.Azure.Storage.Auth;
    using System.Threading.Tasks;
    using System.Threading;

    /// <summary>
    /// A utility class for constructing a <see cref="Microsoft.Azure.Storage.Auth.TokenCredential"/> 
    /// from a <see cref="Azure.Core.TokenCredential"/>.
    /// </summary>
    public static class CredentialShim
    {
        /// <summary>
        /// Creates a <see cref="Microsoft.Azure.Storage.Auth.TokenCredential"/> from the connection info. 
        /// </summary>
        /// <param name="connectionInfo">The connection info.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns></returns>
        public static async Task<Microsoft.Azure.Storage.Auth.TokenCredential> ToLegacyCredentialAsync(this ConnectionInfo connectionInfo, CancellationToken cancellationToken)
        {
            AccessToken token = await GetTokenAsync(connectionInfo.TokenCredential, connectionInfo.Scopes, cancellationToken);
            return new Microsoft.Azure.Storage.Auth.TokenCredential(token.Token, RenewTokenAsync, connectionInfo, NextRefresh(token));
        }

        static ValueTask<AccessToken> GetTokenAsync(
            Azure.Core.TokenCredential credential, string[] scopes, CancellationToken cancellation)
        {
            TokenRequestContext request = new(scopes);
            return credential.GetTokenAsync(request, cancellation);
        }

        static async Task<NewTokenAndFrequency> RenewTokenAsync(object state, CancellationToken cancellationToken)
        {
            ConnectionInfo connectionInfo = (ConnectionInfo)state;
            AccessToken token = await GetTokenAsync(connectionInfo.TokenCredential, connectionInfo.Scopes, cancellationToken);
            return new(token.Token, NextRefresh(token));
        }

        static TimeSpan NextRefresh(AccessToken token)
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            return token.ExpiresOn - now - TimeSpan.FromMinutes(1); // refresh it a bit early.
        }
    }
}
