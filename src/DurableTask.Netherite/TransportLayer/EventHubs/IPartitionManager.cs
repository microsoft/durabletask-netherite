// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System.Threading.Tasks;

    /// <summary>
    /// General interface for starting and stopping the logic that activates the partition and load monitor on hosts.
    /// </summary>
    interface IPartitionManager
    {
        Task StartHostingAsync();

        Task StopHostingAsync();
    }
}
