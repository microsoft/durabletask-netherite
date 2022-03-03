// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading;
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

        public CancellationToken Token => this.cts.Token;
        public bool IsTerminated => this.cts.Token.IsCancellationRequested;

        public bool NormalTermination { get; private set; }

        public PartitionErrorHandler(int partitionId, ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName)
        {
            this.cts = new CancellationTokenSource();
            this.partitionId = partitionId;
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
        }

        public void HandleError(string context, string message, Exception exception, bool terminatePartition, bool isWarning)
        {
            this.TraceError(isWarning, context, message, exception, terminatePartition);

            // terminate this partition in response to the error
            if (terminatePartition && !this.cts.IsCancellationRequested)
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

        public void TerminateNormally()
        {
            this.NormalTermination = true;
            this.Terminate();
        }

        void Terminate()
        {
            try
            {
                this.logger?.LogDebug("Part{partition:D2} Started cancellation");
                this.cts.Cancel();
                this.logger?.LogDebug("Part{partition:D2} Completed cancellation");
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    this.HandleError("PartitionErrorHandler.Terminate", "Encountered exeption while canceling token", e, false, true);
                }
            }
            catch (Exception e)
            {
                this.HandleError("PartitionErrorHandler.Terminate", "Encountered exeption while canceling token", e, false, true);
            }
        }

    }
}
