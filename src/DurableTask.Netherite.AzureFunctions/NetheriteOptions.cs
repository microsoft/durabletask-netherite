// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
