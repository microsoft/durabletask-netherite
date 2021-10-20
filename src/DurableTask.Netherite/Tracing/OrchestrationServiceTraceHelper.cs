// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Text;

    class OrchestrationServiceTraceHelper
    {
        public ILogger Logger { get; private set; }

        public string StorageAccountName { private get; set; } = string.Empty;

        readonly string taskHub;
        readonly LogLevel logLevelLimit;
        readonly string workerName;
        readonly Guid serviceInstanceId;


        public OrchestrationServiceTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, string workerName, string taskHubName)
        {
            this.Logger = loggerFactory.CreateLogger(NetheriteOrchestrationService.LoggerCategoryName);
            this.taskHub = taskHubName;
            this.logLevelLimit = logLevelLimit;
            this.workerName = workerName;
            this.serviceInstanceId = Guid.NewGuid();
        }

        public void TraceCreated(int processorCount, TransportConnectionString.TransportChoices transport, TransportConnectionString.StorageChoices storage)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.Logger.IsEnabled(LogLevel.Information))
                {
                    this.Logger.LogInformation("NetheriteOrchestrationService created, workerId={workerId}, processorCount={processorCount}, transport={transport}, storage={storage}", this.workerName, Environment.ProcessorCount, transport, storage);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.OrchestrationServiceCreated(this.serviceInstanceId, transport.ToString(), storage.ToString(), this.StorageAccountName, this.taskHub, this.workerName, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceStopped()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.Logger.IsEnabled(LogLevel.Information))
                {
                    this.Logger.LogInformation("NetheriteOrchestrationService stopped, workerId={workerId}", this.workerName);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.OrchestrationServiceStopped(this.serviceInstanceId, this.StorageAccountName, this.taskHub, this.workerName, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.Logger.IsEnabled(LogLevel.Information))
                {
                    this.Logger.LogInformation("NetheriteOrchestrationService {details}", details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.OrchestrationServiceProgress(this.StorageAccountName, details, this.taskHub, this.workerName, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.Logger.IsEnabled(LogLevel.Warning))
                {
                    this.Logger.LogInformation("NetheriteOrchestrationService {details}", details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.OrchestrationServiceWarning(this.StorageAccountName, details, this.taskHub, this.workerName, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceError(string message, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                if (this.Logger.IsEnabled(LogLevel.Error))
                {
                    this.Logger.LogError("NetheriteOrchestrationService !!! {message}: {exception}", message, exception);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.OrchestrationServiceError(this.StorageAccountName, message, exception.ToString(), this.taskHub, this.workerName, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceScaleRecommendation(string scaleRecommendation, int workerCount, string reason)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.Logger.IsEnabled(LogLevel.Information))
                {
                    this.Logger.LogInformation("NetheriteOrchestrationService autoscaler recommends: {scaleRecommendation} from: {workerCount} because: {reason}", scaleRecommendation, workerCount, reason);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.NetheriteScaleRecommendation(this.StorageAccountName, this.taskHub, scaleRecommendation, workerCount, reason, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }            
        }
    }
}