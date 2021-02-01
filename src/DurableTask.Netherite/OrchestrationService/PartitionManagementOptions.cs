// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Settings for how the partition load balancer should work.
    /// </summary>
    public enum PartitionManagementOptions
    {
        /// <summary>
        /// Use the orchestration service client only, do not host any partitions.
        /// </summary>
        ClientOnly,

        /// <summary>
        /// Use the event processor host implementation provided by the EventHubs client library.
        /// </summary>
        EventProcessorHost,

        /// <summary>
        /// Follow a predefined partition management script. This is meant to be used for testing and benchmarking scenarios.
        /// </summary>
        Scripted,
    }
}
