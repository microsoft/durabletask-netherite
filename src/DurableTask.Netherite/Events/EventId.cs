// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using Dynamitey;

    /// <summary>
    /// A unique identifier for an event.
    /// </summary>
    public struct EventId
    {
        /// <summary>
        /// The category of an event.
        /// </summary>
        public enum EventCategory
        {
            /// <summary>
            /// An event that is sent from a client to a partition.
            /// </summary>
            ClientRequest,

            /// <summary>
            /// An event that is sent from a partition back to a client, as a response.
            /// </summary>
            ClientResponse,

            /// <summary>
            /// An event that is sent by a partition to itself.
            /// </summary>
            PartitionInternal,

            /// <summary>
            /// An event that is sent from a partition to another partition.
            /// </summary>
            PartitionToPartition,
        }

        /// <summary>
        /// The category of this event
        /// </summary>
        public EventCategory Category { get; set; }

        /// <summary>
        /// For events originating on a client, the client id. 
        /// </summary>
        public Guid ClientId { get; set; }

        /// <summary>
        /// For events originating on a partition, the partition id.
        /// </summary>
        public uint PartitionId { get; set; }

        /// <summary>
        /// For events originating on a client, a sequence number
        /// </summary>
        public long Number { get; set; }

        /// <summary>
        /// For sub-events, the index
        /// </summary>
        public int SubIndex { get; set; }

        /// <summary>
        /// For events originating on a partition, a string for correlating this event
        /// </summary>
        public string WorkItemId { get; set; }

        /// <summary>
        /// For fragmented events, or internal dependent reads, the fragment number or subindex.
        /// </summary>
        public int? Index { get; set; }

        internal static EventId MakeClientRequestEventId(Guid ClientId, long RequestId) => new EventId()
        {
            ClientId = ClientId,
            Number = RequestId,
            Category = EventCategory.ClientRequest
        };

        internal static EventId MakeClientResponseEventId(Guid ClientId, long RequestId) => new EventId()
        {
            ClientId = ClientId,
            Number = RequestId,
            Category = EventCategory.ClientResponse
        };

        internal static EventId MakePartitionInternalEventId(string workItemId) => new EventId()
        {
            WorkItemId = workItemId,
            Category = EventCategory.PartitionInternal
        };

        internal static EventId MakePartitionToPartitionEventId(string workItemId, uint destinationPartition) => new EventId()
        {
            WorkItemId = workItemId,
            PartitionId = destinationPartition,
            Category = EventCategory.PartitionToPartition
        };

        internal static EventId MakeSubEventId(EventId id, int fragment)
        {
            id.Index = fragment;
            return id;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            switch (this.Category)
            {
                case EventCategory.ClientRequest:
                    return $"{Client.GetShortId(this.ClientId)}R{this.Number}{this.IndexSuffix}";

                case EventCategory.ClientResponse:
                    return $"{Client.GetShortId(this.ClientId)}R{this.Number}R{this.IndexSuffix}";

                case EventCategory.PartitionInternal:
                    return $"{this.WorkItemId}{this.IndexSuffix}";

                case EventCategory.PartitionToPartition:
                    return $"{this.WorkItemId}P{this.PartitionId:D2}{this.IndexSuffix}";

                default:
                    throw new InvalidOperationException();
            }
        }

        string IndexSuffix => this.Index.HasValue ? $"I{this.Index.Value}" : string.Empty;
    }
}