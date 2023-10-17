// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
/*
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class EventHubTrigger
    {
        // must set this manually, and comment out the [Disable], to test the trigger
        const int numEntities = 1000;

        [Disable]
        [FunctionName("MyEventHubTrigger")]
        public static async Task RunAsync(
            [EventHubTrigger(Parameters.EventHubName, Connection = "EventHubsConnection")] EventData[] eventDataSet,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            int numTries = 0;

            while (numTries < 10)
            {
                numTries++;

                try
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
                        log.LogDebug($"Sending signal for {evt}");
                        signalTasks.Add(client.SignalEntityAsync(Parameters.GetDestinationEntity(evt, numEntities), nameof(ReceiverEntity.Receive), evt));
                    }

                    log.LogInformation($"Sent {eventDataSet.Length} signals");

                    await Task.WhenAll(signalTasks);

                    break;
                }
                catch (NullReferenceException)
                {
                    // during startup we may get these exceptions if the trigger goes off before the
                    // Orchestration service has been started
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }
    }
*/
}
