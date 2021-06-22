// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Sequence
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A microbenchmark that runs a sequence of tasks. The point is to compare sequence construction using blob-triggers
    /// with an orchestrator doing the same thing.
    /// 
    /// The blob and queue triggers are disabled by default. Uncomment the [Disable] attribute before compilation to use them.
    /// </summary>
    public static class Sequence
    {
        public class Input
        {
            //  For length 3 we would have 3 tasks chained, in this type of sequence:
            //  (createData) --transfer-data--> (doWork) --transfer-data--> (doWork)
            //  The duration is measured from before (createData) to after last (doWork)

            // parameters
            public int Length { get; set; }
            public int WorkExponent { get; set; }
            public int DataExponent { get; set; }

            // data
            public byte[] Data { get; set; }
            public int Position { get; set; }

            public string InstanceId { get; set; }
            public DateTime StartTime { get; set; } // first task starting
            public double Duration { get; set; } // last task ending

            public int DoWork()
            {
                long iterations = 1;
                for (int i = 0; i < this.WorkExponent; i++)
                {
                    iterations *= 10;
                }
                int collisionCount = 0;
                for (long i = 0; i < iterations; i++)
                {
                    if (i.GetHashCode() == 0)
                    {
                        collisionCount++;
                    }
                }
                return collisionCount;
            }

            public byte[] CreateData()
            {
                int numBytes = 1;
                for (int i = 0; i < this.DataExponent; i++)
                {
                    numBytes *= 10;
                }
                var bytes = new byte[numBytes];
                (new Random()).NextBytes(bytes);
                return bytes;
            }
        }

        public static Input RunTask(Input input, ILogger logger, string instanceId)
        {
            if (input.Data == null)
            {
                input.StartTime = DateTime.UtcNow;
                input.Data = input.CreateData();
                input.Position = 0;
                logger.LogWarning($"{instanceId} Producing {input.Data.Length} bytes");
            }
            else
            {
                logger.LogWarning($"{instanceId} Doing work");
                int ignoredResult = input.DoWork();
                input.Position++;
                input.Duration = (DateTime.UtcNow - input.StartTime).TotalSeconds;
                logger.LogWarning($"{instanceId} Passing along {input.Data.Length} bytes");
            }
            return input;
        }
    }
}