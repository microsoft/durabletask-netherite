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
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;

    class OrchestrationWorkItem : TaskOrchestrationWorkItem
    {
        public OrchestrationMessageBatch MessageBatch { get; set; }

        public Partition Partition { get; }

        public ExecutionType Type { get; set; }

        public enum ExecutionType { Fresh, ContinueFromHistory, ContinueFromCursor };

        public OrchestrationWorkItem(Partition partition, OrchestrationMessageBatch messageBatch, List<HistoryEvent> previousHistory = null)
        {
            this.Partition = partition;
            this.MessageBatch = messageBatch;
            this.InstanceId = messageBatch.InstanceId;
            this.NewMessages = messageBatch.MessagesToProcess;
            this.OrchestrationRuntimeState = new OrchestrationRuntimeState(previousHistory);
            this.LockedUntilUtc = DateTime.MaxValue; // this backend does not require workitem lock renewals
            this.Session = null; // we don't use the extended session API because we are caching cursors in the work item
        }

        public void SetNextMessageBatch(OrchestrationMessageBatch messageBatch)
        {
            this.MessageBatch = messageBatch;
            this.NewMessages = messageBatch.MessagesToProcess;
            this.OrchestrationRuntimeState.NewEvents.Clear();
        }
    }
}
