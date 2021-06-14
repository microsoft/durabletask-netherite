// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.ProducerConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;

    /// <summary>
    /// A parameter object passed to the orchestration that starts the test.
    /// </summary>
    [JsonObject(MemberSerialization.OptOut)]
    public class Parameters
    {
        /// <summary>
        /// The number of producer entities.
        /// </summary>
        public int producers { get; set; }

         /// <summary>
        /// The number of consumer entities.
        /// </summary>
        public int consumers { get; set; }

        /// <summary>
        /// THe number of signal batches sent by each producer entity.
        /// </summary>
        public int batches { get; set; }

        /// <summary>
        /// The number of signals contained in each batch.
        /// </summary>
        public int batchsize { get; set; }

        /// <summary>
        /// The size of the signal payload, in bytes
        /// </summary>
        public int messagesize { get; set; }

        /// <summary>
        /// The number of partitions that are used for placing producer entities.
        /// </summary>
        public int producerPartitions { get; set; }

        /// <summary>
        /// The number of partitions that are used for placing consumer entities.
        /// </summary>
        public int consumerPartitions { get; set; }

        /// <summary>
        /// A name for this test, to be used in the results table, and as an entity prefix.
        /// Filled automatically if not explicitly specified.
        /// </summary>
        public string testname { get; set; }

        /// <summary>
        /// A time for keeping entities alive after the test, in minutes. If larger than zero, 
        /// we are pinging all entities on a 15-second interval at the end of the test.
        /// This can be helpful on consumption plans, to ensure logs or telemetry are transmitted before scaling to zero.
        /// </summary>
        public int keepAliveMinutes { get; set; }

        [JsonIgnore]
        public int SignalsPerProducer => this.batches * this.batchsize;

        [JsonIgnore]
        public int TotalNumberOfSignals => this.producers * this.SignalsPerProducer;

        [JsonIgnore]
        public int SignalsPerConsumer => this.TotalNumberOfSignals / this.consumers;

        [JsonIgnore]
        public int TotalVolume => this.TotalNumberOfSignals * this.messagesize;

        public EntityId GetCompletionEntity() => new EntityId(nameof(Completion), $"{this.testname}-!{this.producerPartitions + this.consumerPartitions:D2}");
        public EntityId GetProducerEntity(int i) => new EntityId(nameof(Producer), $"{i:D4}-{this.testname}-!{i % this.producerPartitions:D2}");
        public EntityId GetConsumerEntity(int i) => new EntityId(nameof(Consumer), $"{i:D4}-{this.testname}-!{this.producerPartitions + i % this.consumerPartitions:D2}");
    }
}
