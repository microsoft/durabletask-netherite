// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// An event that is processed by a partition
    /// </summary>
    [DataContract]
abstract class PartitionEvent : Event
    {
        [DataMember]
        public uint PartitionId { get; set; }

        [IgnoreDataMember]
        public ArraySegment<byte> Serialized;

        /// <summary>
        /// For events coming from the input queue, the next input queue position after this event. For internal events, zero.
        /// </summary>
        [DataMember]
        public long NextInputQueuePosition { get; set; }

        [IgnoreDataMember]
        public double ReceivedTimestamp { get; set; }

        [IgnoreDataMember]
        public double ReadyToSendTimestamp { get; set; }

        [IgnoreDataMember]
        public double SentTimestamp { get; set; }

        [IgnoreDataMember]
        public double IssuedTimestamp { get; set; }

        // make a copy of an event so we run it through the pipeline a second time
        public PartitionEvent Clone()
        {
            var evt = (PartitionEvent)this.MemberwiseClone();

            // clear all the non-data fields
            evt.DurabilityListeners.Clear();
            evt.Serialized = default;
            evt.NextInputQueuePosition = 0;

            // clear the timestamps that will be overwritten
            evt.ReadyToSendTimestamp = 0;
            evt.SentTimestamp = 0;
            evt.IssuedTimestamp = 0;

            return evt;
        }

    }
}
