﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        /// <param name="inputQueueFingerprint">A fingerprint for the input queue.</param>
        /// <param name="initialOffset">Initial offset for the input queue, if this partition is being created.</param>
        /// <returns>the input queue position from which to resume input processing</returns>
        /// <exception cref="OperationCanceledException">Indicates that termination was signaled before the operation completed.</exception>
        Task<(long,int)> CreateOrRestoreAsync(Partition localPartition, IPartitionErrorHandler errorHandler, string inputQueueFingerprint, long initialOffset);

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
        /// Queues a read or update event 
        /// for in-order processing on this partition.
        /// </summary>
        /// <param name="evt">The event to process.</param>
        void SubmitEvent(PartitionEvent evt);

        /// <summary>
        /// Queues a list of read or update events
        /// for in-order processing on this partition.
        /// </summary>
        /// <param name="evt">The collection of events to process.</param>
        void SubmitEvents(IList<PartitionEvent> evt);

        /// <summary>
        /// Launches a read or query event 
        /// for immediate processing on this partition, bypassing the queue
        /// </summary>
        /// <param name="evt">The event to process.</param>
        void SubmitParallelEvent(PartitionEvent evt);

        /// <summary>
        /// Prefetches the supplied keys.
        /// </summary>
        /// <returns></returns>
        Task Prefetch(IEnumerable<TrackedObjectKey> keys);
    }
}