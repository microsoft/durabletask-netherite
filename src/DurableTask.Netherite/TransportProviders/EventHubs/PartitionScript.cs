// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    /// Functionality for parsing the partition scripts used by <see cref="ScriptedEventProcessorHost"/>.
    /// </summary>
    static class PartitionScript
    {
        static readonly char[] Separators = new char[] { ' ' };

        public static IEnumerable<ProcessorHostEvent> ParseEvents(DateTime scenarioStartTimeUtc, string workerId, int numPartitions, Stream script)
        {
            int currentTimeSeconds = 0;
            DateTime currentTimeUtc = scenarioStartTimeUtc;

            using (var reader = new StreamReader(script))
            {
                while (true)
                {
                    string line = reader.ReadLine();
                    if (line == null)
                    {
                        break;
                    }

                    var words = line.Split(Separators);

                    if (words[0] == "wait")
                    {
                        var seconds = int.Parse(words[1]);
                        currentTimeSeconds += seconds;
                        currentTimeUtc += TimeSpan.FromSeconds(seconds);
                    }
                    else if (words[1] == workerId || words[1] == "*")
                    {
                        int from;
                        int to;

                        if (words[2] == "*")
                        {
                            (from, to) = (0, numPartitions - 1);
                        }
                        else
                        {
                            from = int.Parse(words[2]);
                            to = (words.Length == 3) ? from : int.Parse(words[3]);
                        }

                        for (int i = from; i <= to; i++)
                        {
                            yield return new ProcessorHostEvent()
                            {
                                TimeSeconds = currentTimeSeconds,
                                TimeUtc = currentTimeUtc,
                                Action = words[0],
                                PartitionId = i,
                            };
                        }
                    }
                }
            }
        }


        /// <summary>
        /// This represents events that the CustomProcessorHost can handle, e.g. starting, stopping, restarting a partition.
        /// </summary>
        public class ProcessorHostEvent
        {
            public DateTime TimeUtc { get; set; }
            public int TimeSeconds { get; set; }
            public string Action { get; set; }
            public int PartitionId { get; set; }
        }
    }
}