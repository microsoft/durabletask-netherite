// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Settings for how to handle speculation.
    /// </summary>
    public enum SpeculationOptions
    {
        /// <summary>
        /// Persist all intermediate results first before starting work-items that depend on them.
        /// </summary>
        None,

        /// <summary>
        /// Allow work items to start locally, even if they depend on not-yet-persisted intermediate results.
        /// </summary>
        Local,

        /// <summary>
        /// Allow work items to start remotely, even if they depend on not-yet-persisted intermediate results.
        /// </summary>
        Global,
    }
}
