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

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    /// <summary>
    /// The result returned by an instance query
    /// </summary>
    [DataContract]
    public class InstanceQueryResult
    {
        /// <summary>
        /// The instances returned by the query.
        /// </summary>
        public IEnumerable<OrchestrationState> Instances { get; set; }

        /// <summary>
        /// A continuation token to resume the query, or null if the results are complete.
        /// </summary>
        public string ContinuationToken { get; set; }
    }
}
