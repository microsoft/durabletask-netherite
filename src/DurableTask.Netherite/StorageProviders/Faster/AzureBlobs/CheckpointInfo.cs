//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------
namespace DurableTask.Netherite.Faster
{
    using Newtonsoft.Json;
    using System;

    [JsonObject]
class CheckpointInfo
    {
        [JsonProperty]
        public Guid IndexToken { get; set; }

        [JsonProperty]
        public Guid LogToken { get; set; }

        [JsonProperty]
        public long CommitLogPosition { get; set; }

        [JsonProperty]
        public long InputQueuePosition { get; set; }

        internal void CopyFrom(CheckpointInfo other)
        {
            this.IndexToken = other.IndexToken;
            this.LogToken = other.LogToken;
            this.CommitLogPosition = other.CommitLogPosition;
            this.InputQueuePosition = other.InputQueuePosition;
        }
    }
}
