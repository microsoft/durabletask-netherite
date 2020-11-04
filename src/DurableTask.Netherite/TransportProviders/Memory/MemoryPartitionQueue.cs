// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Emulated
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.IO;
    using System.Threading;

    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    class MemoryPartitionQueue : MemoryQueue<PartitionEvent, byte[]>, IMemoryQueue<PartitionEvent>
    {
        readonly TransportAbstraction.IPartition partition;

        public MemoryPartitionQueue(TransportAbstraction.IPartition partition, CancellationToken cancellationToken, ILogger logger)
            : base(cancellationToken, $"Part{partition.PartitionId:D2}", logger)
        {
            this.partition = partition;
        }

        protected override byte[] Serialize(PartitionEvent evt)
        {
            var stream = new MemoryStream();
            Packet.Serialize(evt, stream, false, new byte[16]);
            DurabilityListeners.ConfirmDurable(evt);
            return stream.ToArray();
        }

        protected override PartitionEvent Deserialize(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes, false))
            {
                Packet.Deserialize(stream, out PartitionEvent partitionEvent, null);
                return partitionEvent;
            }
        }

        protected override void Deliver(PartitionEvent evt)
        {
            try
            {
                evt.ReceivedTimestamp = this.partition.CurrentTimeMs;

                this.partition.SubmitExternalEvents(new PartitionEvent[] { evt });
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                this.partition.ErrorHandler.HandleError(nameof(MemoryPartitionQueue), $"Encountered exception while trying to deliver event {evt} id={evt.EventIdString}", e, true, false);
            }
        }
    }
}
