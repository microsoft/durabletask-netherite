// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Scaling
{
    /// <summary>
    /// Possible scale actions for durable task hub.
    /// </summary>
    public enum ScaleAction
    {
        /// <summary>
        /// Do not add or remove workers.
        /// </summary>
        None = 0,

        /// <summary>
        /// Add workers to the current task hub.
        /// </summary>
        AddWorker,

        /// <summary>
        /// Remove workers from the current task hub.
        /// </summary>
        RemoveWorker
    }
}
