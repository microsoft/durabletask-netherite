// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Interfaces that separate the transport functionality (which includes both load balancing of partitions
    /// and transmission of messages) from the host, partition, and client components
    /// </summary>
    static class TransportAbstraction
    {
        /// <summary>
        /// The host functionality visible to the transport back-end. 
        /// The transport back-end calls this interface to place clients and partitions on this host.
        /// </summary>
        public interface IHost
        {
            /// <summary>
            /// Assigned by the transport backend to inform the host about the number of partitions.
            /// </summary>
            uint NumberPartitions { set; }

            /// <summary>
            /// Returns the storage provider for storing the partition states.
            /// </summary>
            IStorageProvider StorageProvider { get; }

            /// <summary>
            /// Creates a client on this host.
            /// </summary>
            /// <param name="clientId">A globally unique identifier for this client</param>
            /// <param name="taskHubGuid">the unique identifier of the taskhub</param>
            /// <param name="batchSender">A sender that can be used by the client for sending messages</param>
            /// <returns>A sender for passing messages to the transport backend</returns>
            IClient AddClient(Guid clientId, Guid taskHubGuid, ISender batchSender);

            /// <summary>
            /// Places a partition on this host.
            /// </summary>
            /// <param name="partitionId">The partition id.</param>
            /// <param name="batchSender">A sender for passing messages to the transport backend</param>
            /// <returns></returns>
            IPartition AddPartition(uint partitionId, ISender batchSender);

            /// <summary>
            /// Returns an error handler object for the given partition.
            /// </summary>
            /// <param name="partitionId">The partition id.</param>
            IPartitionErrorHandler CreateErrorHandler(uint partitionId);
        }

        /// <summary>
        /// The partition functionality, as seen by the transport back-end.
        /// </summary>
        public interface IPartition
        {
            /// <summary>
            /// The partition id of this partition.
            /// </summary>
            uint PartitionId { get; }

            /// <summary>
            /// Acquire partition ownership, recover partition state from storage, and resume processing.
            /// </summary>
            /// <param name="termination">A termination object for initiating and/or detecting termination of the partition.</param>
            /// <param name="firstInputQueuePosition">For new partitions, the position of the first message to receive.</param>
            /// <returns>The input queue position of the next message to receive.</returns>
            /// <remarks>
            /// The termination token source can be used for immediately terminating the partition.
            /// Also, it can be used to detect that the partition has terminated for any other reason, 
            /// be it cleanly (after StopAsync) or uncleanly (after losing a lease or hitting a fatal error).
            /// </remarks>
            Task<long> CreateOrRestoreAsync(IPartitionErrorHandler termination, long firstInputQueuePosition);

            /// <summary>
            /// Clean shutdown: stop processing, save partition state to storage, and release ownership.
            /// </summary>
            /// <param name="isForced">True if the shutdown should happen as quickly as possible.</param>
            /// <returns>When all steps have completed and termination is performed.</returns>
            Task StopAsync(bool isForced);

            /// <summary>
            /// Queues a single event for processing on this partition.
            /// </summary>
            /// <param name="partitionEvent">The event to process.</param>
            void SubmitInternalEvent(PartitionUpdateEvent partitionEvent);

            /// <summary>
            /// Queues a batch of incoming external events for processing on this partition.
            /// </summary>
            /// <param name="partitionEvents">The events to process.</param>
            void SubmitExternalEvents(IList<PartitionEvent> partitionEvents);

            /// <summary>
            /// The error handler for this partition.
            /// </summary>
            IPartitionErrorHandler ErrorHandler { get; }

            /// <summary>
            /// The elapsed time in milliseconds since this partition was constructed. We use this
            /// mainly for measuring various timings inside a partition.
            /// </summary>
            double CurrentTimeMs { get; }
        }

        /// <summary>
        /// The client functionality, as seen by the transport back-end.
        /// </summary>
        public interface IClient
        {
            /// <summary>
            /// A unique identifier for this client.
            /// </summary>
            Guid ClientId { get; }

            /// <summary>
            /// Processes a single event on this client.
            /// </summary>
            /// <param name="clientEvent">The event to process.</param>
            void Process(ClientEvent clientEvent);

            /// <summary>
            /// Stop processing events and shut down.
            /// </summary>
            /// <returns>When the client is shut down.</returns>
            Task StopAsync();

            /// <summary>
            /// Indicates an observed error for diagnostic purposes.
            /// </summary>
            /// <param name="msg">A message describing the circumstances.</param>
            /// <param name="e">The exception that was observed.</param>
            void ReportTransportError(string msg, Exception e);
        }

        /// <summary>
        /// A sender abstraction, passed to clients and partitions, for sending messages via the transport.
        /// </summary>
        public interface ISender
        {
            /// <summary>
            /// Send an event. The destination is already determined by the event,
            /// which contains either a client id or a partition id.
            /// </summary>
            /// <param name="element"></param>
            void Submit(Event element);
        }

        /// <summary>
        /// A listener abstraction, used by clients and partitions, to receive acks after events have been
        /// durably processed.
        /// </summary>
        public interface IDurabilityListener
        {
            /// <summary>
            /// Indicates that this event has been durably persisted (incoming events) or sent (outgoing events).
            /// </summary>
            /// <param name="evt">The event that has been durably processed.</param>
            void ConfirmDurable(Event evt);
        }

        /// <summary>
        /// An <see cref="IDurabilityListener"/> that is also listening for exceptions. Used on the client
        /// to make transport errors visible to the calling code.
        /// </summary>
        public interface IDurabilityOrExceptionListener : IDurabilityListener
        {
            /// <summary>
            /// Indicates that there was an error while trying to send this event.
            /// </summary>
            /// <param name="evt"></param>
            /// <param name="e"></param>
            void ReportException(Event evt, Exception e);
        }
    }
}