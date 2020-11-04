// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// A handler for fatal or non-fatal errors encountered in a partition.
    /// </summary>
    public interface IPartitionErrorHandler
    {
        /// <summary>
        /// A cancellation token that is cancelled when the partition is terminated.
        /// </summary>
        CancellationToken Token { get; }

        /// <summary>
        /// A boolean indicating whether the partition is terminated.
        /// </summary>
        bool IsTerminated { get; }

        /// <summary>
        /// Error handling for the partition.
        /// </summary>
        /// <param name="where">A brief description of the component that observed the error.</param>
        /// <param name="message">A message describing the circumstances.</param>
        /// <param name="e">The exception that was observed, or null.</param>
        /// <param name="terminatePartition">whether this partition should be terminated (i.e. recycle and recover from storage).</param>
        /// <param name="reportAsWarning">whether this error should be reported with the severity of a warning.</param>
        void HandleError(string where, string message, Exception e, bool terminatePartition, bool reportAsWarning);

        /// <summary>
        /// Terminates the partition normally, after shutdown.
        /// </summary>
        void TerminateNormally();
    }
}
