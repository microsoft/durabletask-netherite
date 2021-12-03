// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// Functionality for estimating memory size.
    /// </summary>
    class SizeUtils
    {
        public static long GetEstimatedSize(OrchestrationInstance instance)
        {
            return instance == null ? 0 : 32 + 2 * (instance.InstanceId?.Length ?? 0 + instance.ExecutionId?.Length ?? 0);
        }

        public static long GetEstimatedSize(ParentInstance p)
        {
            return p == null ? 0 : 36 + 2 * (p.Name?.Length ?? 0 + p.Version?.Length ?? 0) + GetEstimatedSize(p.OrchestrationInstance);
        }

        public static long GetEstimatedSize(OrchestrationState state)
        {
            //DateTime CompletedTime;
            return state == null ? 0 : 112
                + (state.Status?.Length ?? 0 + state.Output?.Length ?? 0 + state.Name?.Length ?? 0 + state.Input?.Length ?? 0 + (state.Version?.Length ?? 0)) * 2
                + GetEstimatedSize(state.OrchestrationInstance) + GetEstimatedSize(state.ParentInstance);
        }

        public static long GetEstimatedSize(HistoryState state)
        {
            //TODO
            return 100 + 50 * (state.History?.Count ?? 0);
        }
    }
}
