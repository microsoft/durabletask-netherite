// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Text;

    class LoadMonitorTraceHelper
    {
        readonly ILogger logger;
        readonly string account;
        readonly string taskHub;
        readonly LogLevel logLevelLimit;

        public LoadMonitorTraceHelper(ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName)
        {
            this.logger = logger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.logLevelLimit = logLevelLimit;
        }

        public void TraceProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogInformation("LoadMonitor {details}", details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.LoadMonitorProgress(this.account, this.taskHub, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    this.logger.LogWarning("LoadMonitor {details}", details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.LoadMonitorWarning(this.account, this.taskHub, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceError(string message, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                if (this.logger.IsEnabled(LogLevel.Error))
                {
                    this.logger.LogError("LoadMonitor !!! {message}: {exception}", message, exception);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.LoadMonitorError(this.account, this.taskHub, message, exception.ToString(), TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }
    }
}