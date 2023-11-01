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
    /// A utility class for terminating the partition if some task takes too long.
    /// Implemented as a disposable, with <see cref="IAsyncDisposable.DisposeAsync"> used to cancel the timeout.
    /// </summary>
    class PartitionTimeout : IAsyncDisposable
    {
        readonly CancellationTokenSource tokenSource;
        readonly Task timeoutTask;

        public PartitionTimeout(IPartitionErrorHandler errorHandler, string task, TimeSpan timeout)
        {
            this.tokenSource = new CancellationTokenSource();
            this.timeoutTask = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(timeout, this.tokenSource.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // we did not time out
                    return;
                }

                errorHandler.HandleError(
                    $"{nameof(PartitionTimeout)}",
                    $"{task} timed out after {timeout}",
                    e: null,
                    terminatePartition: true,
                    reportAsWarning: false);
            });
        }

        public async ValueTask DisposeAsync()
        {      
            // cancel the timeout task (if it has not already completed)
            this.tokenSource.Cancel();

            // wait for the timeouttask to complete here, so we can be sure that the
            // decision about the timeout firing or not firing has been made
            // before we leave this method
            await this.timeoutTask.ConfigureAwait(false);

            // we can dispose the token source now since the timeoutTask is completed
            this.tokenSource.Dispose();
        }
    }
}
