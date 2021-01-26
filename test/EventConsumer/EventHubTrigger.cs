// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace EventConsumer
{
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class EventHubTrigger
    {

        [FunctionName("MyEventHubTrigger")]
        public static async Task RunAsync(
            [EventHubTrigger(TestConstants.EventHubName, Connection = "EventHubsConnection")] EventData[] eventDataSet,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            if (TestConstants.BatchSignals)
            {
                // we pack all the events into a byte sequence, and then
                // send those bytes as a signals to the entity

                var stream = new MemoryStream();
                using var streamWriter = new BinaryWriter(stream);

                for (int i = 0; i < eventDataSet.Length; i++)
                {
                    var eventData = eventDataSet[i];
                    var sp = eventData.SystemProperties;
                    streamWriter.Write((int) eventData.Body[0]);
                    streamWriter.Write(eventData.SystemProperties.SequenceNumber);
                    streamWriter.Write(eventData.Body.Count);
                    streamWriter.Write(eventData.Body);
                }

                streamWriter.Flush();
                await client.SignalEntityAsync(new EntityId(nameof(ReceiverEntity), "0"), nameof(ReceiverEntity.ReceiveBatch), stream.ToArray());
            }
            else
            {
                var signalTasks = new List<Task>();

                // send one signal for each packet
                for (int i = 0; i < eventDataSet.Length; i++)
                {
                    var eventData = eventDataSet[i];
                    var sp = eventData.SystemProperties;
                    byte[] payload = eventData.Body.ToArray();
                    var evt = new Event()
                    { 
                        Partition = payload[0],
                        SeqNo = eventData.SystemProperties.SequenceNumber,
                        Payload = payload
                    };
                    log.LogInformation($"Sending signal for {evt}");
                    signalTasks.Add(client.SignalEntityAsync(new EntityId(nameof(ReceiverEntity), "0"), nameof(ReceiverEntity.Receive), evt));
                }

                await Task.WhenAll(signalTasks);
            }
        }
    }
}
