// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// A "partition manager" for diagnostic purposes, which recovers one or more partitions, but does not modify any files.
    /// </summary>
    class RecoveryTester : IPartitionManager
    {
        readonly TransportAbstraction.IHost host;
        readonly TaskhubParameters parameters;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly EventHubsTraceHelper logger;
        readonly List<uint> partitionsToTest;
        readonly List<TransportAbstraction.IPartition> partitions;

        public RecoveryTester(
            TransportAbstraction.IHost host,
            TaskhubParameters parameters,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper logger)
        {
            this.host = host;
            this.parameters = parameters;
            this.settings = settings;
            this.logger = logger;
            this.ParseParameters(out this.partitionsToTest);
            this.partitions = new List<TransportAbstraction.IPartition>();
        }

        public async Task StartHostingAsync()
        {
            this.logger.LogWarning($"RecoveryTester is testing partition recovery, parameters={this.settings.PartitionManagementParameters ?? "null"}");

            this.ParseParameters(out List<uint> partitionsToTest);

            foreach(uint partitionId in partitionsToTest) 
            {    
                try
                {
                    this.logger.LogInformation("RecoveryTester is starting partition {partitionId}", partitionId);

                    // start this partition (which may include waiting for the lease to become available)
                    var partition = this.host.AddPartition(partitionId, new NullSender());

                    var errorHandler = this.host.CreateErrorHandler(partitionId);

                    var nextPacketToReceive = await partition.CreateOrRestoreAsync(errorHandler, this.parameters, "").ConfigureAwait(false);
                    
                    this.logger.LogInformation("RecoveryTester recovered partition {partitionId} successfully", partitionId);

                    this.partitions.Add(partition);
                }
                catch (Exception e)
                {
                    this.logger.LogError("RecoveryTester failed while recovering partition {partitionId}: {exception}", partitionId, e);
                }
            }

            this.logger.LogWarning($"RecoveryTester completed testing partition recovery");
        }

        public async Task StopHostingAsync()
        {
            foreach (var partition in this.partitions)
            {
                try
                {
                    this.logger.LogInformation("RecoveryTester is stopping partition {partitionId}", partition.PartitionId);

                    await partition.StopAsync(true);

                    this.logger.LogInformation("RecoveryTester stopped partition {partitionId} successfully", partition.PartitionId);
                }
                catch (Exception e)
                {
                    this.logger.LogError("RecoveryTester failed while stopping partition {partitionId}: {exception}", partition.PartitionId, e);
                }
            }
        }

        public void ParseParameters(out List<uint> partitionsToTest)
        {
            partitionsToTest = new List<uint>();
            if (this.settings.PartitionManagementParameters == null)
            {
                // test all partitions
                for (uint i = 0; i < this.parameters.PartitionCount; i++)
                {
                    partitionsToTest.Add(i);
                }
            }
            else
            {
                // test only one partition as specified in this.settings.PartitionManagementParameters
                try
                {
                    int num = int.Parse(this.settings.PartitionManagementParameters);
                    if (num < 0 || num >= this.parameters.PartitionCount)
                    {
                        throw new IndexOutOfRangeException("partition out of range");
                    }
                    partitionsToTest.Add((uint) num);

                }
                catch (Exception ex)
                {
                    throw new NetheriteConfigurationException($"invalid parameter for {nameof(RecoveryTester)}: {this.settings.PartitionManagementParameters}", ex);
                }
            }
        }

        class NullSender: TransportAbstraction.ISender
        {
            public void Submit(Event element)
            {
            }
        }
    }
}
