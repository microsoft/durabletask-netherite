//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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
