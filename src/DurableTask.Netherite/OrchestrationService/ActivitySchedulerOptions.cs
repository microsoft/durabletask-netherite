// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Settings for how activities .
    /// </summary>
    public enum ActivitySchedulerOptions
    {
        /// <summary>
        /// All activities are scheduled on the same partition as the orchestration.
        /// </summary>
        Local,

        /// <summary>
        /// Activities are scheduled locally if possible, but backlog is offloaded periodically.
        /// </summary>
        PeriodicOffload,
    }
}
