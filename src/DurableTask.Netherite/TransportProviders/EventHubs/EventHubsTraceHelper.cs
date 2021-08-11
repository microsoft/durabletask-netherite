// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubs
{
    using Microsoft.Extensions.Logging;
    using System;

    /// <summary>
    /// Trace helpers for the event hubs transport.
    /// </summary>
    class EventHubsTraceHelper : ILogger, IBatchWorkerTraceHelper
    {
        readonly ILogger logger;
        readonly string partitionId; // is null for host-level, but is non-null for partition eventprocessor
        readonly string account;
        readonly string taskHub;
        readonly string eventHubsNamespace;
        readonly LogLevel logLevelLimit;

        public static ILogger CreateLogger(ILoggerFactory loggerFactory)
        {
            return loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.EventHubsTransport");
        }

        public EventHubsTraceHelper(ILogger logger, LogLevel logLevelLimit, uint? partitionId, string storageAccountName, string taskHubName, string eventHubsNamespace)
        {
            this.logger = logger;
            this.partitionId = partitionId?.ToString() ?? string.Empty;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.eventHubsNamespace = eventHubsNamespace;
            this.logLevelLimit = logLevelLimit;
        }

        public EventHubsTraceHelper(EventHubsTraceHelper traceHelper, uint partitionId)
            : this(traceHelper.logger, traceHelper.logLevelLimit, partitionId, traceHelper.account, traceHelper.taskHub, traceHelper.eventHubsNamespace)
        { 
        }

        public bool IsEnabled(LogLevel logLevel) => logLevel >= this.logLevelLimit;
     
        public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

        class NoopDisposable : IDisposable
        {
            public static NoopDisposable Instance = new NoopDisposable();
            public void Dispose()
            { }
        }

        public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            // quit if not enabled
            if (this.logLevelLimit <= logLevel)
            {
                // pass through to the ILogger
                this.logger.Log(logLevel, eventId, state, exception, formatter);

                // additionally, if etw is enabled, pass on to ETW   
                if (EtwSource.Log.IsEnabled())
                {
                    string details = formatter(state, exception);

                    switch (logLevel)
                    {
                        case LogLevel.Trace:
                            EtwSource.Log.EventHubsTrace(this.account, this.taskHub, this.eventHubsNamespace, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Debug:
                            EtwSource.Log.EventHubsDebug(this.account, this.taskHub, this.eventHubsNamespace, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Information:
                            EtwSource.Log.EventHubsInformation(this.account, this.taskHub, this.eventHubsNamespace, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Warning:
                            EtwSource.Log.EventHubsWarning(this.account, this.taskHub, this.eventHubsNamespace, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Error:
                        case LogLevel.Critical:
                            EtwSource.Log.EventHubsError(this.account, this.taskHub, this.eventHubsNamespace, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        default:
                            break;
                    }
                }
            }
        }

        public void TraceBatchWorkerProgress(string worker, int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
            // used only at host level
            System.Diagnostics.Debug.Assert(this.partitionId == null);

            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("{worker} completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds:F2} nextBatch={nextBatch}",
                   worker, batchSize, elapsedMilliseconds, nextBatch.ToString() ?? "");
            }

            EtwSource.Log.BatchWorkerProgress(this.account, this.taskHub, null, worker, batchSize, elapsedMilliseconds, nextBatch.ToString() ?? "", TraceUtils.AppName, TraceUtils.ExtensionVersion);
        }
    }
}