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
        LocalOnly,

        /// <summary>
        /// Activities are scheduled remotely.
        /// </summary>
        RemoteOnly,

        /// <summary>
        /// Activities are scheduled locally and remotely.
        /// </summary>
        Mixed,
    }
}
