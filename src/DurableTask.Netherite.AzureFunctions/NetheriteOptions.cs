// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using DurableTask.Netherite;
    using Newtonsoft.Json;

    public class NetheriteOptions
    {
        [JsonProperty("connectionStringName")]
        public string ConnectionStringName { get; set; } = "UseDevelopmentStorage=true;";

        [JsonProperty("taskEventLockTimeout")]
        public TimeSpan TaskEventLockTimeout { get; set; } = TimeSpan.FromMinutes(2);

        internal NetheriteOrchestrationServiceSettings ProviderOptions { get; set; } = new NetheriteOrchestrationServiceSettings();
    }
}
