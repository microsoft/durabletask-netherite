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
    /// Implemented as a disposable, with <see cref="IDisposable.Dispose"> used to cancel the timeout.
    /// </summary>
    class PartitionTimeout : IDisposable
    {
        readonly CancellationTokenSource tokenSource;
        readonly Task timeoutTask;

        public PartitionTimeout(IPartitionErrorHandler errorHandler, string task, TimeSpan timeout)
        {
            this.tokenSource = new CancellationTokenSource();
            this.timeoutTask = Task.Delay(timeout, this.tokenSource.Token);

            // if the timeout tasks runs to completion without being cancelled, terminate the partition
            this.timeoutTask.ContinueWith(
                 _ => errorHandler.HandleError(
                     $"{nameof(PartitionTimeout)}",
                     $"{task} timed out after {timeout}",
                     e: null,
                     terminatePartition: true,
                     reportAsWarning: false),
                 TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        public void Dispose()
        {      
            // cancel the timeout task (if it has not already completed)
            this.tokenSource.Cancel();

            // dispose the token source after the timeout task has completed
            this.timeoutTask.ContinueWith(_ => this.tokenSource.Dispose());
        }
    }
}
