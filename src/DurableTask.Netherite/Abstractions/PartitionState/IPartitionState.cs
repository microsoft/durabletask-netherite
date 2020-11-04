// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Abstraction for a storage-backed, event-sourced partition.
    /// </summary>
    interface IPartitionState
    {
        /// <summary>
        /// Restore the state of a partition from storage, or create a new one if there is nothing stored.
        /// </summary>
        /// <param name="localPartition">The partition.</param>
        /// <param name="errorHandler">An error handler to initiate and/or indicate termination of this partition.</param>
        /// <param name="firstInputQueuePosition">For new partitions, the position of the first message to receive.</param>
        /// <returns>the input queue position from which to resume input processing</returns>
        /// <exception cref="OperationCanceledException">Indicates that termination was signaled before the operation completed.</exception>
        Task<long> CreateOrRestoreAsync(Partition localPartition, IPartitionErrorHandler errorHandler, long firstInputQueuePosition);

        /// <summary>
        /// Starts processing, after creating or restoring the partition state.
        /// </summary>
        void StartProcessing();

        /// <summary>
        /// Finish processing events and save the partition state to storage.
        /// </summary>
        /// <param name="takeFinalCheckpoint">Whether to take a final state checkpoint.</param>
        /// <returns>A task that completes when the state has been saved.</returns>
        /// <exception cref="OperationCanceledException">Indicates that termination was signaled before the operation completed.</exception>
        Task CleanShutdown(bool takeFinalCheckpoint);

        /// <summary>
        /// Queues an internal event (originating on this same partition)
        /// for processing on this partition.
        /// </summary>
        /// <param name="evt">The event to process.</param>
        void SubmitInternalEvent(PartitionEvent evt);

        /// <summary>
        /// Queues external events (originating on clients or other partitions)
        /// for processing on this partition.
        /// </summary>
        /// <param name="evt">The collection of events to process.</param>
        void SubmitExternalEvents(IList<PartitionEvent> evt);
    }
}