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

namespace DurableTask.Netherite.Scaling
{
    /// <summary>
    /// Possible scale actions for durable task hub.
    /// </summary>
    public enum ScaleAction
    {
        /// <summary>
        /// Do not add or remove workers.
        /// </summary>
        None = 0,

        /// <summary>
        /// Add workers to the current task hub.
        /// </summary>
        AddWorker,

        /// <summary>
        /// Remove workers from the current task hub.
        /// </summary>
        RemoveWorker
    }
}
