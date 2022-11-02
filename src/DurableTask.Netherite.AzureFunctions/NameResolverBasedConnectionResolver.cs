// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Identity;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging.Abstractions;

#if !NETCOREAPP2_2

    /// <summary>
    /// Resolves connections using a token credential and a mapping from connection names to resource names.
    /// </summary>
    class NameResolverBasedConnectionNameResolver : DurableTask.Netherite.ConnectionNameToConnectionStringResolver
    {
        public NameResolverBasedConnectionNameResolver(INameResolver nameResolver)
            : base((string name) => nameResolver.Resolve(name))
        {      
        }
    }

#endif
}