// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace EventConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public class TestConstants
    {
        /// <summary>
        /// The name of the Event Hub that connects the consumer and producer
        /// </summary>
        public const string EventHubName = "eventstest";

        /// <summary>
        /// Whether to deliver all events in the batch as a single entity signal, or as individual signals
        /// </summary>
        public static bool BatchSignals => true;

        /// <summary>
        /// Whether to use the class-based API, or the function-based API for entities.
        /// </summary>
        public static bool UseClassBasedAPI => true;

        /// <summary>
        /// The number of events to produce or consume, in one run of the test.
        /// </summary>
        public static int NumberEventsPerTest => 10000;

        /// <summary>
        /// The size of the random payload to attach to each event, in bytes
        /// </summary>
        public static long PayloadSize => 1000;

        /// <summary>
        /// The number of events to include in each batch on the producer side
        /// </summary>
        public static int ProducerBatchSize => 500;
    }
}
