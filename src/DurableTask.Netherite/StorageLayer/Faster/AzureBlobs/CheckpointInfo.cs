// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


namespace DurableTask.Netherite.Faster
{
    using Newtonsoft.Json;
    using System;

    [JsonObject]
    class CheckpointInfo
    {
        /// <summary>
        /// The FasterKV token for the last index checkpoint taken before this checkpoint.
        /// </summary>
        [JsonProperty]
        public Guid IndexToken { get; set; }

        /// <summary>
        /// The FasterKV token for this checkpoint.
        /// </summary>
        [JsonProperty]
        public Guid LogToken { get; set; }

        /// <summary>
        /// The FasterLog position for this checkpoint.
        /// </summary>
        [JsonProperty]
        public long CommitLogPosition { get; set; }

        /// <summary>
        /// The input queue (event hubs) position for this checkpoint.
        /// </summary>
        [JsonProperty]
        public long InputQueuePosition { get; set; }

        /// <summary>
        /// If the input queue position is a batch, the position within the batch.
        /// </summary>
        [JsonProperty]
        public int InputQueueBatchPosition { get; set; }

        /// <summary>
        /// The input queue fingerprint for this checkpoint.
        /// </summary>
        [JsonProperty]
        public string InputQueueFingerprint { get; set; }

        /// <summary>
        /// The number of recovery attempts that have been made for this checkpoint.
        /// </summary>
        //[JsonProperty]
        public int RecoveryAttempts { get; set; }
    }
}
