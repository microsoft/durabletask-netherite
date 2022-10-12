// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.SingleHostTransport
{
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Azure;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An in-memory queue for delivering events.
    /// </summary>
    class PartitionQueue : BatchWorker<PartitionEvent>, TransportAbstraction.IDurabilityListener
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly uint partitionId;
        readonly string fingerPrint;
        readonly TestHooks testHooks;
        readonly TaskhubParameters parameters;
        readonly ILogger logger;

        long position;
        readonly Queue<PartitionEvent> redeliverQueue;
        long redeliverQueuePosition;
        long ackedBefore;

        bool isShuttingDown;

        readonly byte[] taskhubGuid = new byte[16];

        public TransportAbstraction.IPartition Partition { get; private set; }

        public PartitionQueue(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            uint partitionId,
            string fingerPrint,
            TestHooks testHooks,
            TaskhubParameters parameters,
            ILogger logger)
            : base($"PartitionQueue{partitionId:D2}", false, int.MaxValue, CancellationToken.None, null)
        {
            this.host = host;
            this.sender = sender;
            this.partitionId = partitionId;
            this.fingerPrint = fingerPrint;
            this.testHooks = testHooks;
            this.parameters = parameters;
            this.logger = logger;
            this.position = 0;

            this.redeliverQueue = new Queue<PartitionEvent>();
            this.redeliverQueuePosition = 0;
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            if (this.isShuttingDown)
            {
                if (this.Partition != null)
                {
                    await this.Partition.StopAsync(false);
                }
                return;
            }

            if (this.Partition == null || this.Partition.ErrorHandler.IsTerminated)
            {
                this.Partition = this.host.AddPartition(this.partitionId, this.sender);
                var errorHandler = this.host.CreateErrorHandler(this.partitionId);
                errorHandler.OnShutdown += () =>
                {
                    if (!this.isShuttingDown && this.testHooks?.FaultInjectionActive != true)
                    {
                        this.testHooks.Error("MemoryTransport", "Unexpected partition termination");
                    }
                    this.Notify();
                };
                
                var nextInputQueuePosition = await this.Partition.CreateOrRestoreAsync(errorHandler, this.parameters, this.fingerPrint);

                while(this.redeliverQueuePosition < nextInputQueuePosition)
                {
                    this.redeliverQueue.Dequeue();
                    this.redeliverQueuePosition++;
                }

                this.ackedBefore = nextInputQueuePosition;

                if (nextInputQueuePosition < this.position)
                {
                    // redeliver the missing events
                    var redeliverList = new List<PartitionEvent>();
                    var taskhubGuid = new byte[16];
                    this.Deliver(this.redeliverQueue.Take((int)(this.position - nextInputQueuePosition)), nextInputQueuePosition);
                }
            }

            if (batch.Count > 0)
            {
                using var stream = new MemoryStream();

                this.Deliver(batch, this.position);
                this.position += batch.Count;

                foreach (var evt in batch)
                {
                    this.redeliverQueue.Enqueue(evt);
                    DurabilityListeners.ConfirmDurable(evt);
                }
            }

            while (this.redeliverQueuePosition < Interlocked.Read(ref this.ackedBefore))
            {
                this.redeliverQueue.Dequeue();
                this.redeliverQueuePosition++;
            }
        }

        public Task StopAsync()
        {
            this.isShuttingDown = true;
            return this.WaitForCompletionAsync();
        }

        void Deliver(IEnumerable<PartitionEvent> evts, long position)
        {
            //using var stream = new MemoryStream();
            var list = new List<PartitionEvent>();
            foreach (var evt in evts)
            {
                // serialize and deserialize to make a copy that clears all temporary data
                var stream = new MemoryStream(); 
                stream.Seek(0, SeekOrigin.Begin);
                Packet.Serialize(evt, stream, this.taskhubGuid);
                stream.Seek(0, SeekOrigin.Begin);
                Packet.Deserialize(stream, out PartitionEvent freshEvent, null);
                DurabilityListeners.Register(freshEvent, this);
                freshEvent.NextInputQueuePosition = ++position;
                list.Add(freshEvent);
            }
            this.Partition.SubmitEvents(list);
        }

        public void ConfirmDurable(Event evt)
        {
            var partitionEvent = (PartitionEvent)evt;

            long current = Interlocked.Read(ref this.ackedBefore);
            if (current < partitionEvent.NextInputQueuePosition)
            {
                Interlocked.Exchange(ref this.ackedBefore, partitionEvent.NextInputQueuePosition);
            }
        }
    }
}
