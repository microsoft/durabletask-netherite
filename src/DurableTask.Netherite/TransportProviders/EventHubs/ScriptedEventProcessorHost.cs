// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using DurableTask.Core.Common;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An alternate event processor host where partitions are placed (started and stopped)
    /// according to a script that is read from a blob, as opposed to automatically load balanced.
    /// It is intended for benchmarking and testing scenarios only, not production.
    /// </summary>
    class ScriptedEventProcessorHost
    {
        readonly string eventHubPath;
        readonly string consumerGroupName;
        readonly string eventHubConnectionString;
        readonly string storageConnectionString;
        readonly string leaseContainerName;
        readonly string workerId;
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly EventHubsConnections connections;
        readonly TaskhubParameters parameters;
        readonly byte[] taskHubGuid;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly EventHubsTraceHelper logger;
        readonly List<PartitionInstance> partitionInstances = new List<PartitionInstance>();

        int numberOfPartitions;

        public ScriptedEventProcessorHost(
            string eventHubPath,
            string consumerGroupName,
            string eventHubConnectionString,
            string storageConnectionString,
            string leaseContainerName,
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            EventHubsConnections connections,
            TaskhubParameters parameters,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper logger,
            string workerId)
        {
            this.eventHubPath = eventHubPath;
            this.consumerGroupName = consumerGroupName;
            this.eventHubConnectionString = eventHubConnectionString;
            this.storageConnectionString = storageConnectionString;
            this.leaseContainerName = leaseContainerName;
            this.host = host;
            this.sender = sender;
            this.connections = connections;
            this.parameters = parameters;
            this.taskHubGuid = parameters.TaskhubGuid.ToByteArray();
            this.settings = settings;
            this.logger = logger;
            this.workerId = workerId;
        }

        public void StartEventProcessing(NetheriteOrchestrationServiceSettings settings, CloudBlockBlob partitionScript)
        {
            if (!partitionScript.Exists())
            {
                this.logger.LogInformation("ScriptedEventProcessorHost workerId={workerId} is waiting for script", this.workerId);
                while (! partitionScript.Exists())
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }

            // we use the UTC modification timestamp on the script as the scenario start time
            DateTime scenarioStartTimeUtc = partitionScript.Properties.LastModified.Value.UtcDateTime;

            // the number of partitions matters only if the script contains wildcards
            this.numberOfPartitions = this.parameters.StartPositions.Length;
            for (var partitionIndex = 0; partitionIndex < this.numberOfPartitions; partitionIndex++)
            {
                this.partitionInstances.Add(null);
            }

            List<PartitionScript.ProcessorHostEvent> timesteps = new List<PartitionScript.ProcessorHostEvent>(); ;

            try
            {
                using (var memoryStream = new System.IO.MemoryStream())
                {
                    partitionScript.DownloadRangeToStream(memoryStream, null, null);
                    memoryStream.Seek(0, System.IO.SeekOrigin.Begin);
                    timesteps.AddRange(PartitionScript.ParseEvents(scenarioStartTimeUtc, settings.WorkerId, this.numberOfPartitions, memoryStream));
                }

                this.logger.LogInformation("ScriptedEventProcessorHost workerId={workerId} started.", this.workerId);
            }
            catch(Exception e)
            {
                this.logger.LogError($"ScriptedEventProcessorHost workerId={this.workerId} failed to parse partitionscript: {e}");
            }

            int nextTime = 0;
            List<PartitionScript.ProcessorHostEvent> nextGroup = new List<PartitionScript.ProcessorHostEvent>();

            foreach (var timestep in timesteps)
            {
                if (nextTime == timestep.TimeSeconds)
                {
                    nextGroup.Add(timestep);
                }
                else
                {
                    this.Process(nextGroup);
                    nextGroup.Clear();
                    nextGroup.Add(timestep);
                    nextTime = timestep.TimeSeconds;
                }
            }

            this.Process(nextGroup);
        }

        public Task StopAsync()
        {
            // TODO implement this. Not urgent since this class is currently only used for testing/benchmarking
            return Task.CompletedTask;
        }

        void Process(List<PartitionScript.ProcessorHostEvent> ready)
        {
            if (ready.Count > 0)
            {
                int delay = (int)(ready[0].TimeUtc - DateTime.UtcNow).TotalMilliseconds;
                if (delay > 0)
                {
                    this.logger.LogInformation("ScriptedEventProcessorHost workerId={workerId} is waiting for {delay} ms until next hostEvent", this.workerId, delay);
                    Thread.Sleep(delay);
                }

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                bool parallel = true;

                var tasks = new List<Task>();
                int lasttime = 0;
                foreach (var timestep in ready)
                {
                    this.logger.LogWarning("ScriptedEventProcessorHost workerId={workerId} performs action={action} partition={partition} time={time}.", this.workerId, timestep.Action, timestep.PartitionId, timestep.TimeSeconds);
                    lasttime = timestep.TimeSeconds;
                }
                foreach (var timestep in ready)
                {
                    if (parallel)
                    {
                        tasks.Add(this.ProcessHostEvent(timestep));
                    }
                    else
                    {
                        this.ProcessHostEvent(timestep).GetAwaiter().GetResult();
                    }
                }
                Task.WhenAll(tasks).GetAwaiter().GetResult();
                this.logger.LogWarning("ScriptedEventProcessorHost workerId={workerId} finished all actions for time={time} in {elapsedSeconds}s.", this.workerId, lasttime, stopwatch.Elapsed.TotalSeconds);
            }
        }

        async Task ProcessHostEvent(PartitionScript.ProcessorHostEvent timestep)
        {
            try
            {
                int partitionId = timestep.PartitionId;
                if (timestep.Action == "restart")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    var newPartitionInstance = new PartitionInstance((uint) partitionId, oldPartitionInstance.Incarnation + 1, this);
                    this.partitionInstances[partitionId] = newPartitionInstance;
                    await Task.WhenAll(newPartitionInstance.StartAsync(), oldPartitionInstance.StopAsync());
                }
                else if (timestep.Action == "start")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    var newPartitionInstance = new PartitionInstance((uint)partitionId, (oldPartitionInstance?.Incarnation ?? 0) + 1, this);
                    this.partitionInstances[partitionId] = newPartitionInstance;
                    await newPartitionInstance.StartAsync();
                }
                else if (timestep.Action == "stop")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    await oldPartitionInstance.StopAsync();
                }
                else
                {
                    throw new InvalidOperationException($"Unknown action: {timestep.Action}");
                }

                this.logger.LogWarning("ScriptedEventProcessorHost workerId={workerId} successfully performed action={action} partition={partition} time={time}.", this.workerId, timestep.Action, timestep.PartitionId, timestep.TimeSeconds);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                // TODO: Maybe in the future we would like to actually do something in case of failure. 
                //       For now it is fine to ignore them.
                this.logger.LogError("ScriptedEventProcessorHost workerId={workerId} failed on action={action} partition={partition} time={time} exception={exception}", this.workerId, timestep.Action, timestep.PartitionId, timestep.TimeSeconds, e);
            }
        }

        /// <summary>
        /// Represents a particular instance of a partition that is being managed by a CustomEventProcessor host.
        /// </summary>
        class PartitionInstance
        {
            readonly uint partitionId;
            readonly ScriptedEventProcessorHost host;

            TransportAbstraction.IPartition partition;
            Task partitionEventLoop;
            PartitionReceiver partitionReceiver;
            CancellationTokenSource shutdownSource;
            Task shutdownTask;
            // Just copied from EventHubsTransport
            const int MaxReceiveBatchSize = 1000; // actual batches are typically much smaller

            public PartitionInstance(uint partitionId, int incarnation, ScriptedEventProcessorHost eventProcessorHost)
            {
                this.partitionId = partitionId;
                this.Incarnation = incarnation;
                this.host = eventProcessorHost;
            }

            public int Incarnation { get; }

            public async Task StartAsync()
            {
                this.shutdownSource = new CancellationTokenSource();
                this.shutdownTask = this.WaitForShutdownAsync();

                try
                {
                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) is starting partition", this.host.eventHubPath, this.partitionId, this.Incarnation);

                    // start this partition (which may include waiting for the lease to become available)
                    this.partition = this.host.host.AddPartition(this.partitionId, this.host.sender);

                    var errorHandler = this.host.host.CreateErrorHandler(this.partitionId);

                    var nextPacketToReceive = await this.partition.CreateOrRestoreAsync(errorHandler, this.host.parameters.StartPositions[Convert.ToInt32(this.partitionId)]).ConfigureAwait(false);
                    this.host.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) started partition, next expected packet is #{nextSeqno}", this.host.eventHubPath, this.partitionId, this.Incarnation, nextPacketToReceive);

                    this.partitionEventLoop = Task.Run(() => this.PartitionEventLoop(nextPacketToReceive));
                }
                catch(Exception e) when (!Utils.IsFatal(e))
                {
                    this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) failed to start partition: {exception}", this.host.eventHubPath, this.partitionId, this.Incarnation, e);
                    throw;
                }
            }

            async Task WaitForShutdownAsync()
            {
                if (!this.shutdownSource.IsCancellationRequested)
                {
                    var tcs = new TaskCompletionSource<object>();
                    var registration = this.shutdownSource.Token.Register(() =>
                     {
                         tcs.TrySetResult(true);
                     });
                    await tcs.Task;
                    registration.Dispose();
                }
            }

            // TODO: Handle errors
            public async Task StopAsync()
            {
                try
                {
                    // First stop the partition. We need to wait until it shutdowns before closing the receiver, since it needs to receive confirmation events.
                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopping partition)", this.host.eventHubPath, this.partitionId, this.Incarnation);
                    await this.partition.StopAsync(false).ConfigureAwait(false);
                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopped partition", this.host.eventHubPath, this.partitionId, this.Incarnation);

                    // wait for the receiver loop to terminate
                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopping receiver loop", this.host.eventHubPath, this.partitionId, this.Incarnation);
                    this.shutdownSource.Cancel();
                    await this.partitionEventLoop.ConfigureAwait(false);

                    // shut down the partition receiver (eventHubs complains if more than 5 of these are active per partition)
                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) closing the partition receiver", this.host.eventHubPath, this.partitionId, this.Incarnation);
                    await this.partitionReceiver.CloseAsync().ConfigureAwait(false);

                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopped partition", this.host.eventHubPath, this.partitionId, this.Incarnation);
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) failed to stop partition: {exception}", this.host.eventHubPath, this.partitionId, this.Incarnation, e);
                    throw;
                }
            }

            // TODO: Update all the logging messages
            async Task PartitionEventLoop(long nextPacketToReceive)
            {
                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) starting receive loop", this.host.eventHubPath, this.partitionId, this.Incarnation);
                try
                {
                    this.partitionReceiver = this.host.connections.CreatePartitionReceiver((int)this.partitionId, this.host.consumerGroupName, nextPacketToReceive);

                    while (!this.shutdownSource.IsCancellationRequested)
                    {
                        this.host.logger.LogTrace("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) trying to receive eventdata from position {position}", this.host.eventHubPath, this.partitionId, this.Incarnation, nextPacketToReceive);

                        IEnumerable<EventData> eventData;

                        try
                        {
                            var receiveTask = this.partitionReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromMinutes(1));
                            await Task.WhenAny(receiveTask, this.shutdownTask).ConfigureAwait(false);
                            this.shutdownSource.Token.ThrowIfCancellationRequested();
                            eventData = await receiveTask.ConfigureAwait(false);
                        }
                        catch (TimeoutException exception)
                        {
                            // not sure that we should be seeing this, but we do.
                            this.host.logger.LogWarning("Retrying after transient(?) TimeoutException in ReceiveAsync {exception}", exception);
                            eventData = null;
                        }

                        if (eventData != null)
                        {
                            this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received eventdata from position {position}", this.host.eventHubPath, this.partitionId, this.Incarnation, nextPacketToReceive);

                            var batch = new List<PartitionEvent>();
                            var receivedTimestamp = this.partition.CurrentTimeMs;

                            foreach (var eventDatum in eventData)
                            {
                                var seqno = eventDatum.SystemProperties.SequenceNumber;
                                if (seqno == nextPacketToReceive)
                                {
                                    PartitionEvent partitionEvent = null;
                                    try
                                    {
                                        Packet.Deserialize(eventDatum.Body, out partitionEvent, this.host.taskHubGuid);
                                    }
                                    catch (Exception)
                                    {
                                        this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) could not deserialize packet #{seqno} ({size} bytes)", this.host.eventHubPath, this.partitionId, this.Incarnation, seqno, eventDatum.Body.Count);
                                        throw;
                                    }

                                    nextPacketToReceive = seqno + 1;

                                    if (partitionEvent != null)
                                    {
                                        this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received packet #{seqno} ({size} bytes) {event}", this.host.eventHubPath, this.partitionId, this.Incarnation, seqno, eventDatum.Body.Count, partitionEvent);
                                    }
                                    else
                                    {
                                        this.host.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation})  ignored packet #{seqno} for different taskhub", this.host.eventHubPath, this.partitionId, this.Incarnation, seqno);
                                        continue;
                                    }

                                    partitionEvent.NextInputQueuePosition = nextPacketToReceive;
                                    batch.Add(partitionEvent);
                                    partitionEvent.ReceivedTimestamp = this.partition.CurrentTimeMs;
                                }
                                else if (seqno > nextPacketToReceive)
                                {
                                    this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received wrong packet, #{seqno} instead of #{expected}", this.host.eventHubPath, this.partitionId, this.Incarnation, seqno, nextPacketToReceive);
                                    // this should never happen, as EventHubs guarantees in-order delivery of packets
                                    throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                                }
                                else
                                {
                                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) discarded packet #{seqno} because it is already processed", this.host.eventHubPath, this.partitionId, this.Incarnation, seqno);
                                }
                            }

                            if (batch.Count > 0 && !this.shutdownSource.IsCancellationRequested)
                            {
                                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.host.eventHubPath, this.partitionId, this.Incarnation, batch.Count, batch[0].NextInputQueuePosition - 1, nextPacketToReceive);
                                this.partition.SubmitExternalEvents(batch);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    this.host.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) was terminated", this.host.eventHubPath, this.partitionId, this.Incarnation);
                }
                catch (Exception exception)
                {
                    this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) encountered an exception while processing packets : {exception}", this.host.eventHubPath, this.partitionId, this.Incarnation, exception);
                    this.partition.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
                }

                this.host.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) ReceiverLoop exits", this.host.eventHubPath, this.partitionId, this.Incarnation);
            }
        }
    }
}
