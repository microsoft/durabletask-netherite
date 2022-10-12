// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Identity;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

#if !NETCOREAPP2_2           

    /// <summary>
    /// Allows customization of what settings to use for what connection. Can be used to specify TokenCredentials. 
    /// </summary>
    public abstract class ConnectionSettingsCustomizer
    {
        /// <summary>
        /// Update the connection settings for the Netherite orchestration service.
        /// Is called before the service is started. Can also be called for client-only connections.
        /// </summary>
        /// <param name="settings">The settings.</param>
        void UpdateNetheriteSettings(string taskhub, string connectionName, NetheriteOrchestrationServiceSettings settings)
        {
            return;
        }
    }

#endif
}
