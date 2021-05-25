// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Settings for how to handle persistence.
    /// </summary>
    public enum PersistenceOptions
    {
        /// <summary>
        /// Persist all intermediate results first before starting work-items that depend on them.
        /// </summary>
        Conservative,

        /// <summary>
        /// Allow work items to start locally, even if they depend on not-yet-persisted intermediate results.
        /// </summary>
        LocallyPipelined,

        /// <summary>
        /// Allow work items to start remotely, even if they depend on not-yet-persisted intermediate results.
        /// </summary>
        GloballyPipelined,
    }
}
