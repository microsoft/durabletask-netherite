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
    using Microsoft.Extensions.Logging;
    using System;
    using System.IO;
    using System.Threading;

    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    class MemoryClientQueue : MemoryQueue<ClientEvent, byte[]>, IMemoryQueue<ClientEvent>
    {
        readonly TransportAbstraction.IClient client;

        public MemoryClientQueue(TransportAbstraction.IClient client, CancellationToken cancellationToken, ILogger logger)
            : base(cancellationToken, $"Client.{Client.GetShortId(client.ClientId)}", logger)
        {
            this.client = client;
        }

        protected override byte[] Serialize(ClientEvent evt)
        {
            var stream = new MemoryStream();
            Packet.Serialize(evt, stream, false, new byte[16]);
            return stream.ToArray();
        }

        protected override ClientEvent Deserialize(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes, false))
            {
                Packet.Deserialize(stream, out ClientEvent clientEvent, null);
                return clientEvent;
            }
        }

        protected override void Deliver(ClientEvent evt)
        {
            try
            {
                this.client.Process(evt);
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                this.client.ReportTransportError(nameof(MemoryClientQueue), e);
            }
        }
    }
}
