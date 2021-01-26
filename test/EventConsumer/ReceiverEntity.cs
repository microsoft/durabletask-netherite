// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace EventConsumer
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    [JsonObject(MemberSerialization.OptIn)]
    public class ReceiverEntity
    {
        ILogger logger;
        bool objectPreviouslyUsed;

        [JsonProperty]
        public int EventCount { get; set; }

        [JsonProperty]
        public int ConstructionCount { get; set; }

        [JsonProperty]
        public int BatchCount { get; set; }

        [JsonProperty]
        public DateTime StartTime { get; set; }

        [JsonProperty]
        public Dictionary<int, long> LastReceived { get; set; }

        [JsonProperty]
        public int OutOfOrderCount { get; set; }

        public ReceiverEntity(ILogger logger)
        {
            this.logger = logger;
        }

        public ILogger Logger { set { this.logger = value; } }

        public void ReceiveBatch(byte[] bytes)
        {
            this.BatchCount++;
            var stream = new MemoryStream(bytes);
            using var reader = new BinaryReader(stream);
            while (stream.Position < bytes.Length)
            {
                int partition = reader.ReadInt32();
                long seqNo = reader.ReadInt64();
                int length = reader.ReadInt32();
                byte[] payload = reader.ReadBytes(length);
                this.Receive(new Event()
                {
                    Partition = partition,
                    SeqNo = seqNo,
                    Payload = payload
                });
            }
        }

        public void Receive(Event evt)
        {
            if (this.EventCount == 0)
            {
                this.logger.LogWarning("Starting test");
                this.StartTime = DateTime.UtcNow;
                this.LastReceived = new Dictionary<int, long>();
            }

            if (this.logger.IsEnabled(LogLevel.Information))
            {
                this.logger.LogInformation($"Received event #{this.EventCount}: {evt}");
            }

            // test in-order delivery
            if (this.LastReceived.TryGetValue(evt.Partition, out long lastSeqNo))
            {
                if (lastSeqNo + 1 != evt.SeqNo)
                {
                    this.logger.LogError($"out-of-order delivery: expecting seqno={lastSeqNo + 1} but received {evt}");
                    this.OutOfOrderCount++;
                }
                this.LastReceived[evt.Partition] = evt.SeqNo;
            }
            else
            {
                this.LastReceived[evt.Partition] = evt.SeqNo;
            }
            
            // increment counter
            this.EventCount++;

            // track how many times we have constructed this object
            if (!this.objectPreviouslyUsed)
            {
                this.ConstructionCount++;
                this.objectPreviouslyUsed = true;
            }

            if (this.EventCount == TestConstants.NumberEventsPerTest)
            {
                this.logger.LogWarning($"Completed test, elapsed={(DateTime.UtcNow - this.StartTime).TotalSeconds:f2}s, eventCount={this.EventCount}, batchCount={this.BatchCount} constructionCount={this.ConstructionCount} outOfOrderCount={this.OutOfOrderCount}");
                this.EventCount = 0;
                this.BatchCount = 0;
                this.ConstructionCount = 0;
                this.LastReceived = null;
                this.OutOfOrderCount = 0;
            }
        }

        [FunctionName(nameof(ReceiverEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            if (TestConstants.UseClassBasedAPI)
            {
                return context.DispatchAsync<ReceiverEntity>(logger);
            }
            else
            {
                var state = context.GetState<ReceiverEntity>(() => new ReceiverEntity(logger));

                state.Logger = logger;

                switch (context.OperationName)
                {
                    case (nameof(ReceiverEntity.ReceiveBatch)):
                        state.ReceiveBatch(context.GetInput<byte[]>());
                        break;
                    case (nameof(ReceiverEntity.Receive)):
                        state.Receive(context.GetInput<Event>());
                        break;

                }

                return Task.CompletedTask;
            }
        }
    }
}
 