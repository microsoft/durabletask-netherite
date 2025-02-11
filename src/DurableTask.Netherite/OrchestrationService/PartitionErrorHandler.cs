// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using Microsoft.Extensions.Logging;

    // For indicating and initiating termination, and for tracing errors and warnings relating to a partition.
    // Is is basically a wrapper around CancellationTokenSource with features for diagnostics.
    class PartitionErrorHandler : IPartitionErrorHandler
    {
        readonly CancellationTokenSource cts = new CancellationTokenSource();
        readonly int partitionId;
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly TaskCompletionSource<object> shutdownComplete;
        readonly TransportAbstraction.IHost host;
        readonly List<Task> disposeTasks;

        public CancellationToken Token
        {
            get
            {
                try
                {
                    return this.cts.Token;
                }
                catch (ObjectDisposedException)
                {
                    return new CancellationToken(true);
                }
            }
        }

        public bool IsTerminated => this.terminationStatus != NotTerminated;

        public bool NormalTermination =>  this.terminationStatus == TerminatedNormally;

        public bool WaitForDisposeTasks(TimeSpan timeout)
        {
            return Task.WhenAll(this.disposeTasks).Wait(timeout);
        }

        volatile int terminationStatus = NotTerminated;
        const int NotTerminated = 0;
        const int TerminatedWithError = 1;
        const int TerminatedNormally = 2;

        public PartitionErrorHandler(int partitionId, ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName, TransportAbstraction.IHost host)
        {
            this.cts = new CancellationTokenSource();
            this.partitionId = partitionId;
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.shutdownComplete = new TaskCompletionSource<object>();
            this.host = host;
            this.disposeTasks = new List<Task>();
        }
     
        public void HandleError(string context, string message, Exception exception, bool terminatePartition, bool isWarning)
        {
            bool isFatal = exception != null && Utils.IsFatal(exception);

            isWarning = isWarning && !isFatal;
            terminatePartition = terminatePartition || isFatal;

            this.TraceError(isWarning, context, message, exception, terminatePartition);

            // if necessary, terminate this partition in response to the error
            if (terminatePartition && this.terminationStatus == NotTerminated)
            {
                if (Interlocked.CompareExchange(ref this.terminationStatus, TerminatedWithError, NotTerminated) == NotTerminated)
                {
                    this.Terminate();
                }
            }

            // if fatal, notify host, because it may start a quick shutdown path in response
            if (isFatal)
            {
                this.host.OnFatalExceptionObserved(exception); 
            }
        }

        public void TerminateNormally()
        {
            if (Interlocked.CompareExchange(ref this.terminationStatus, TerminatedNormally, NotTerminated) == NotTerminated)
            {
                this.Terminate();
            }
        }

        void TraceError(bool isWarning, string context, string message, Exception exception, bool terminatePartition)
        {
            var logLevel = isWarning ? LogLevel.Warning : LogLevel.Error;
            if (this.logLevelLimit <= logLevel)
            {
                // for warnings, do not print the entire exception message
                string details = exception == null ? string.Empty : (isWarning ? $"{exception.GetType().FullName}: {exception.Message}" : exception.ToString());
                
                this.logger?.Log(logLevel, "Part{partition:D2} !!! {message} in {context}: {details} terminatePartition={terminatePartition}", this.partitionId, message, context, details, terminatePartition);

                if (isWarning)
                {
                    EtwSource.Log.PartitionWarning(this.account, this.taskHub, this.partitionId, context, terminatePartition, message, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
                else
                {
                    EtwSource.Log.PartitionError(this.account, this.taskHub, this.partitionId, context, terminatePartition, message, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        void Terminate()
        {
            try
            {
                // this immediately cancels all activities depending on the error handler token
                this.cts.Cancel();
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    this.HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
                }
            }
            catch (Exception e)
            {
                this.HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
            }
            finally
            {
                // now that the partition is dead, run all the dispose tasks
                Task.Run(this.DisposeAsync);
            }
        }

        public void AddDisposeTask(string name, TimeSpan timeout, Action action)
        {
            this.disposeTasks.Add(new Task(() =>
            {
                Task disposeTask = Task.Run(action);
                try
                {
                    bool completedInTime = disposeTask.Wait(timeout);
                    if (!completedInTime)
                    {
                        this.HandleError("PartitionErrorHandler.DisposeAsync", $"Dispose Task {name} timed out after {timeout}", null, false, false);
                    }
                }
                catch(Exception exception)
                {
                    this.HandleError("PartitionErrorHandler.DisposeAsync", $"Dispose Task {name} threw exception {exception}", null, false, false);
                }
            }));
        }

        async Task DisposeAsync()
        {
            // execute all the dispose tasks in parallel
            var tasks = this.disposeTasks;
            foreach (var task in tasks)
            {
                task.Start();
            }
            await Task.WhenAll(tasks);

            // we can now dispose the cancellation token source itself
            this.cts.Dispose();
        }
    }
}
