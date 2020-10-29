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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides an async lock (does not block waiting threads)
    /// </summary>
    class AsyncLock : SemaphoreSlim, IDisposable
    {
        readonly AcquisitionToken token; 

        public AsyncLock() : base(1, 1)
        {
            this.token = new AcquisitionToken()
            {
                AsyncLock = this
            };
        }

        public async ValueTask<AcquisitionToken> LockAsync()
        {
            await base.WaitAsync();
            return this.token;
        }

        internal struct AcquisitionToken : IDisposable
        {
            public AsyncLock AsyncLock;

            public void Dispose()
            {
                this.AsyncLock.Release();
            }
        }        
    }
}
