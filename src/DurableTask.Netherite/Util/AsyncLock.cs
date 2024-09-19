// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        readonly CancellationToken shutdownToken;

        public AsyncLock(CancellationToken shutdownToken) : base(1, 1)
        {
            this.shutdownToken = shutdownToken;
            this.token = new AcquisitionToken()
            {
                AsyncLock = this
            };
        }

        public async ValueTask<AcquisitionToken> LockAsync()
        {
            try
            {
                await base.WaitAsync();
            }
            catch (ObjectDisposedException) when (this.shutdownToken.IsCancellationRequested)
            {
                this.shutdownToken.ThrowIfCancellationRequested();
            }

            return this.token;
        }

        void ReleaseSemaphore()
        {
            try
            {
                base.Release();
            }
            catch (ObjectDisposedException) when (this.shutdownToken.IsCancellationRequested)
            {
                this.shutdownToken.ThrowIfCancellationRequested();
            }
        }

        internal struct AcquisitionToken : IDisposable
        {
            public AsyncLock AsyncLock;

            public void Dispose()
            {
                this.AsyncLock.ReleaseSemaphore();
            }
        }        
    }
}
