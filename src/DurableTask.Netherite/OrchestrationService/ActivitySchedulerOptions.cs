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
        /// Activities in the local backlog are immediately evenly offloaded to other partitions
        /// </summary>
        Static,

        /// <summary>
        /// Activities are load balanced with a global load monitor that issues transfer commands
        /// </summary>
        Locavore,
    }

    public static class ActivityScheduling
    {
        public static bool RequiresPeriodicOffloadDecision(ActivitySchedulerOptions options)
        {
            switch (options)
            {
                case ActivitySchedulerOptions.Local:
                case ActivitySchedulerOptions.Locavore:
                    return false;
                case ActivitySchedulerOptions.Static:
                    return true;
                default:
                    throw new NotImplementedException("missing switch case");
            }
        }   
    }
}
