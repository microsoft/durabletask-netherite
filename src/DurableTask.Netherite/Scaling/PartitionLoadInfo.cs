// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;

    /// <summary>
    /// Reported load information about a specific partition.
    /// </summary>
    [DataContract]
    public class PartitionLoadInfo
    {
        /// <summary>
        /// The number of orchestration work items waiting to be processed.
        /// </summary>
        [DataMember]
        public int WorkItems { get; set; }

        /// <summary>
        /// The number of activities that are waiting to be processed.
        /// </summary>
        [DataMember]
        public int Activities { get; set; }

        /// <summary>
        /// The number of timers that are waiting to fire.
        /// </summary>
        [DataMember]
        public int Timers { get; set; }

        /// <summary>
        /// The number of client requests waiting to be processed.
        /// </summary>
        [DataMember]
        public int Requests { get; set; }

        /// <summary>
        /// The number of work items that have messages waiting to be sent.
        /// </summary>
        [DataMember]
        public int Outbox { get; set; }

        /// <summary>
        /// The total number of orchestration and entity instances
        /// </summary>
        [DataMember]
        public long Instances { get; set; }

        /// <summary>
        /// The next time on which to wake up.
        /// </summary>
        [DataMember]
        public DateTime? Wakeup { get; set; }

        /// <summary>
        /// Checks if this partition has pending work
        /// </summary>
        /// <returns>a description of some of the work, or null if no work</returns>
        public string IsBusy()
        {
            if (this.Activities > 0)
            {
                return $"has {this.Activities} activities pending";
            }

            if (this.WorkItems > 0)
            {
                return $"has {this.WorkItems} work items pending";
            }

            if (this.Requests > 0)
            {
                return $"has {this.Requests} requests pending";
            }

            if (this.Outbox > 0)
            {
                return $"has {this.Outbox} unsent messages";
            }

            if (this.Wakeup.HasValue && this.Wakeup.Value < DateTime.UtcNow + TimeSpan.FromSeconds(20))
            {
                return $"has timer waking up at {this.Wakeup.Value}";
            }

            return null;
        }

        /// <summary>
        /// The input queue position of this partition, which is  the next expected EventHubs sequence number.
        /// </summary>
        [DataMember]
        public long InputQueuePosition { get; set; }

        /// <summary>
        /// The commit log position of this partition.
        /// </summary>
        [DataMember]
        public long CommitLogPosition { get; set; }

        /// <summary>
        /// The worker id of the host that is currently running this partition.
        /// </summary>
        [DataMember]
        public string WorkerId { get; set; }

        /// <summary>
        /// A string encoding of the latency trend.
        /// </summary>
        [DataMember]
        public string LatencyTrend { get; set; }

        /// <summary>
        /// Percentage of message batches that miss in the cache.
        /// </summary>
        [DataMember]
        public double MissRate { get; set; }

        /// <summary>
        /// The current size of the cache, as a percentage.
        /// </summary>
        [DataMember]
        public int Cache { get; set; }

        /// <summary>
        /// The current size of the cache in MB
        /// </summary>
        [DataMember]
        public double CacheMB { get; set; }

        /// <summary>
        /// The character representing idle load.
        /// </summary>
        public const char Idle = 'I';

        /// <summary>
        /// The character representing low latency.
        /// </summary>
        public const char LowLatency = 'L';

        /// <summary>
        /// The character representing medium latency.
        /// </summary>
        public const char MediumLatency = 'M';

        /// <summary>
        /// The character representing high latency.
        /// </summary>
        public const char HighLatency = 'H';

        /// <summary>
        /// All of the latency category characters in order
        /// </summary>
        public static char[] LatencyCategories = new char[] { Idle, LowLatency, MediumLatency, HighLatency };

        /// <summary>
        /// The maximum length of the latency trend
        /// </summary>
        public static int LatencyTrendLength = 5;

        /// <summary>
        /// Whether a latency trend indicates the partition has been idle for a long time
        /// </summary>
        public static bool IsLongIdle(string latencyTrend) => latencyTrend.Count() == LatencyTrendLength && latencyTrend.All(c => c == Idle);

        /// <summary>
        /// Whether a latency trend indicates that a partition is experiencing significant load
        /// </summary>
        public bool IsLoaded()
        {
            var last = this.LatencyTrend.LastOrDefault();
            return (last == MediumLatency || last == HighLatency);
        }

        /// <summary>
        /// Copy the load info for the next measuring interval
        /// </summary>
        /// <returns></returns>
        public static PartitionLoadInfo FirstFrame(string workerId)
        {
            return new PartitionLoadInfo()
            {
                InputQueuePosition = 0,
                CommitLogPosition = 0,
                WorkerId = workerId,
                LatencyTrend = Idle.ToString(),
            };
        }

        /// <summary>
        /// Copy the load info for the next measuring interval
        /// </summary>
        /// <returns></returns>
        public PartitionLoadInfo NextFrame()
        {
            var copy = new PartitionLoadInfo()
            {
                InputQueuePosition = this.InputQueuePosition,
                CommitLogPosition = this.CommitLogPosition,
                WorkerId = this.WorkerId,
                LatencyTrend = this.LatencyTrend,
            };

            if (copy.LatencyTrend.Length == PartitionLoadInfo.LatencyTrendLength)
            {
                copy.LatencyTrend = $"{copy.LatencyTrend.Substring(1)}{Idle}";
            }
            else
            {
                copy.LatencyTrend = $"{copy.LatencyTrend}{Idle}";
            }

            return copy;
        }

        public void MarkActive()
        {
            char last = this.LatencyTrend[this.LatencyTrend.Length-1];
            if (last == Idle)
            {
                this.LatencyTrend = $"{this.LatencyTrend.Substring(0, this.LatencyTrend.Length-1)}{LowLatency}"; 
            }
        }

        public void MarkMediumLatency()
        {
            char last = this.LatencyTrend[this.LatencyTrend.Length - 1];
            if (last == Idle || last == LowLatency)
            {
                this.LatencyTrend = $"{this.LatencyTrend.Substring(0, this.LatencyTrend.Length - 1)}{MediumLatency}"; 
            }
        }

        public void MarkHighLatency()
        {
            char last = this.LatencyTrend[this.LatencyTrend.Length - 1];
            if (last == Idle || last == LowLatency || last == MediumLatency)
            {
                this.LatencyTrend = $"{this.LatencyTrend.Substring(0, this.LatencyTrend.Length - 1)}{HighLatency}";
            }
        }
    }
}
