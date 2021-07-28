// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using Microsoft.Extensions.Logging;
    using System;

    /// <summary>
    /// Trace helpers for the workers.
    /// </summary>
    class WorkerTraceHelper : ILogger
    {
        readonly ILogger logger;
        readonly string workerId;
        readonly string account;
        readonly string taskHub;
        readonly string eventHubsNamespace;
        readonly LogLevel logLevelLimit;

        public static ILogger CreateLogger(ILoggerFactory loggerFactory)
        {
            return loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.Worker");
        }

        public WorkerTraceHelper(ILogger logger, LogLevel logLevelLimit, Guid workerId, string storageAccountName, string taskHubName, string eventHubsNamespace)
        {
            this.logger = logger;
            this.workerId = workerId.ToString();
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.eventHubsNamespace = eventHubsNamespace;
            this.logLevelLimit = logLevelLimit;
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
                            EtwSource.Log.EventHubsTrace(this.account, this.taskHub, this.eventHubsNamespace, this.workerId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Debug:
                            EtwSource.Log.EventHubsDebug(this.account, this.taskHub, this.eventHubsNamespace, this.workerId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Information:
                            EtwSource.Log.EventHubsInformation(this.account, this.taskHub, this.eventHubsNamespace, this.workerId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Warning:
                            EtwSource.Log.EventHubsWarning(this.account, this.taskHub, this.eventHubsNamespace, this.workerId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        case LogLevel.Error:
                        case LogLevel.Critical:
                            EtwSource.Log.EventHubsError(this.account, this.taskHub, this.eventHubsNamespace, this.workerId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                            break;

                        default:
                            break;
                    }
                }
            }
        }
    }
}