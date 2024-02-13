// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

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
        /// Adds a task to be executed after the partition has been terminated.
        /// </summary>
        void AddDisposeTask(string name, TimeSpan timeout, Action action);

        /// <summary>
        /// A boolean indicating whether the partition is terminated.
        /// </summary>
        bool IsTerminated { get; }

        /// <summary>
        /// A boolean indicating that normal termination has been initiated as part of a shutdown.
        /// </summary>
        bool NormalTermination { get; }

        /// <summary>
        /// Waits for the dispose tasks to complete, up to the specified time limit.
        /// </summary>
        /// <param name="timeout">The maximum time to wait for</param>
        /// <returns>true if all tasks completed within the timeout, false otherwise.</returns>
        bool WaitForDisposeTasks(TimeSpan timeout);

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
