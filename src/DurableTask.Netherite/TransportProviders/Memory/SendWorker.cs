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

namespace DurableTask.Netherite.Emulated
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    class SendWorker : BatchWorker<Event>, TransportAbstraction.ISender
    {
        Action<IEnumerable<Event>> sendHandler;

        public SendWorker(CancellationToken token)
            : base(nameof(SendWorker), token)
        {
        }

        public void SetHandler(Action<IEnumerable<Event>> sendHandler)
        {
            this.sendHandler = sendHandler ?? throw new ArgumentNullException(nameof(sendHandler));
        }

        void TransportAbstraction.ISender.Submit(Event element)
        {
            this.Submit(element);
        }

        protected override Task Process(IList<Event> batch)
        {
            if (batch.Count > 0)
            {
                try
                {
                    this.sendHandler(batch);
                }
                catch (Exception e)
                {
                    System.Diagnostics.Trace.TraceError($"exception in send worker: {e}", e);
                }
            }

            return Task.CompletedTask;
        }
    }
}
