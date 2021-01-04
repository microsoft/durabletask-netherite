// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace EventProducer
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using EventConsumer;

    class Program
    {
        // Run this locally, prior to starting the function up, to fill the selected EventHub or AzureStorage queue
        // with the events for the test. Then start function app to consume the events.

        // When running function app multiple times, clear AzureStorage:
        // - EventHubs: clear the blobs in 
        //      azure-webjobs-eventhub/XXX.servicebus.windows.net/YYY
        //      where XXX = eventhubs namespace, YYY = eventhub 
        //   This means that events are consumed from the EventHub beginning again on next start.
        //   This also means that you only need to run Generate.exe once to fill up the EventHub
        //     (because it does not remove messages (immediately, anyway.. it eventually ages them out)).

        const string NoPrompt = "NoPrompt";
        static async Task Main(string[] args)
        {
            var connectionStringName = "EventHubsConnection";

            bool getConnectionString(out string connStr)
            {
                connStr = Environment.GetEnvironmentVariable(connectionStringName);
                return !string.IsNullOrWhiteSpace(connStr);
            }

            if (!getConnectionString(out string connectionString))
            {
                Console.Error.WriteLine($"ConnectionStringName environment variable {connectionStringName} was not found or did not contain a valid connection string");
                return;
            }

            await GenerateEventsInEventHub(
                connectionString, 
                numEvents: TestConstants.NumberEventsPerTest, 
                messageSize: TestConstants.PayloadSize, 
                batchSize: TestConstants.ProducerBatchSize);
 
            //Console.WriteLine("; press any key to exit\n");
            //Console.ReadKey();
        }

        static async Task GenerateEventsInEventHub(string connectionString, int numEvents, long messageSize, int batchSize)
        {
            Console.WriteLine($"Generating {numEvents} EventHub events");

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
            {
                EntityPath = TestConstants.EventHubName,
            };
            var ehc = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            async Task SendEventsAsync(int offset, int events, string partitionId)
            {
                var partitionSender = ehc.CreatePartitionSender(partitionId);
                var eventBatch = new List<EventData>(100);
                for (int x = 0; x < events; x++)
                {
                    byte[] payload = new byte[messageSize];
                    (new Random()).NextBytes(payload);
                    payload[0] = byte.Parse(partitionId);
                    var e = new EventData(payload);
                    eventBatch.Add(e);

                    if (x % batchSize == (batchSize - 1))
                    {
                        await partitionSender.SendAsync(eventBatch);
                        Console.WriteLine($"Sent {x + 1} events to partition {partitionId}");
                        eventBatch = new List<EventData>(100);
                    }
                }
                if (eventBatch.Count > 0)
                {
                    await partitionSender.SendAsync(eventBatch);
                    Console.WriteLine($"Sent {events} events to partition {partitionId}");
                }
            }

            var ehInfo = ehc.GetRuntimeInformationAsync().GetAwaiter().GetResult();
            Console.WriteLine($"Sending {numEvents} events to {ehInfo.PartitionCount} partitions...");

            var partitionTasks = new List<Task>();
            var remaining = numEvents;
            for (int i = ehInfo.PartitionCount; i >= 1; i--)
            {
                var portion = remaining / i;
                remaining -= portion;
                partitionTasks.Add(SendEventsAsync(remaining, portion, ehInfo.PartitionIds[i - 1]));
            }

            await Task.WhenAll(partitionTasks);

            Console.WriteLine($"Generated all {numEvents} EventHub events.");
        }
    }
}
