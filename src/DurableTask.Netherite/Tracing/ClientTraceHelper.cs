// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Text;

    class ClientTraceHelper
    {
        readonly ILogger logger;
        readonly string account;
        readonly string taskHub;
        readonly Guid clientId;
        readonly LogLevel logLevelLimit;
        readonly string tracePrefix;

        public LogLevel LogLevelLimit => this.logLevelLimit;

        public ClientTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, string storageAccountName, string taskHubName, Guid clientId)
        {
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.Client");
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.clientId = clientId;
            this.logLevelLimit = logLevelLimit;
            this.tracePrefix = $"Client.{Client.GetShortId(clientId)}";
        }

        public void TraceProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.logger.IsEnabled(LogLevel.Information))
                {
                    this.logger.LogInformation("{client} {details}", this.tracePrefix, details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientProgress(this.account, this.taskHub, this.clientId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceError(string context, string message, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                if (this.logger.IsEnabled(LogLevel.Error))
                {
                    this.logger.LogError("{client} !!! {message}: {exception}", this.tracePrefix, message, exception);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientError(this.account, this.taskHub, this.clientId, context, message, exception.ToString(), TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceTimerProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    this.logger.LogTrace("{client} {details}", this.tracePrefix, details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientTimerProgress(this.account, this.taskHub, this.clientId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceRequestTimeout(EventId partitionEventId, uint partitionId)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    this.logger.LogWarning("{client} Request {eventId} for partition {partitionId:D2} timed out", this.tracePrefix, partitionEventId, partitionId);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientRequestTimeout(this.account, this.taskHub, this.clientId, partitionEventId.ToString(), (int) partitionId, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceSend(PartitionEvent @event)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("{client} Sending event {eventId}: {event}", this.tracePrefix, @event.EventIdString, @event);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientSentEvent(this.account, this.taskHub, this.clientId, @event.EventIdString, @event.ToString(), TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        public enum ResponseType {  Fragment, PartialQ, CompleteQ, Response };

        public void TraceReceive(ClientEvent @event, ResponseType status)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("{client} Processing event id={eventId} {status}: {event}", this.tracePrefix, @event.EventIdString, status, @event);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientReceivedEvent(this.account, this.taskHub, this.clientId, @event.EventIdString, status.ToString(), @event.ToString(), TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }
    }
}