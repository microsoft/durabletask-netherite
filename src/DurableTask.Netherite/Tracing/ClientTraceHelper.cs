// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

        public ClientTraceHelper(ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName, Guid clientId)
        {
            this.logger = logger;
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
                    EtwSource.Log.ClientProgress(this.account, this.taskHub, this.clientId, details, TraceUtils.ExtensionVersion);
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
                    EtwSource.Log.ClientError(this.account, this.taskHub, this.clientId, context, message, exception.ToString(), TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceTimerProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("{client} {details}", this.tracePrefix, details);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientTimerProgress(this.account, this.taskHub, this.clientId, details, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceRequestTimeout(EventId eventId, uint partitionId)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    this.logger.LogWarning("{client} Request {eventId} for partition {partitionId:D2} timed out", this.tracePrefix, eventId, partitionId);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientRequestTimeout(this.account, this.taskHub, this.clientId, eventId.ToString(), (int) partitionId, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceSend(Event @event)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("{client} Sending event {eventId}: {event}", this.tracePrefix, @event.EventIdString, @event);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientEventSent(this.account, this.taskHub, this.clientId, @event.EventIdString, @event.ToString(), TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceReceive(Event @event)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("{client} Processing event {eventId}: {event}", this.tracePrefix, @event.EventIdString, @event);
                }
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.ClientEventReceived(this.account, this.taskHub, this.clientId, @event.EventIdString, @event.ToString(), TraceUtils.ExtensionVersion);
                }
            }
        }
    }
}