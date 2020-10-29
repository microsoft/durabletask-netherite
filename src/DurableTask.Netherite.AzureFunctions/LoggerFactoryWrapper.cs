// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.IO;
    using System.Threading;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Logging;

    class LoggerFactoryWrapper : ILoggerFactory
    {
        readonly ILoggerFactory loggerFactory;
        readonly NetheriteProviderFactory providerFactory;
        readonly string hubName;
        readonly string workerId;

        public LoggerFactoryWrapper(ILoggerFactory loggerFactory, string hubName, string workerId, NetheriteProviderFactory providerFactory)
        {
            this.hubName = hubName;
            this.workerId = workerId;
            this.loggerFactory = loggerFactory;
            this.providerFactory = providerFactory;
        }

        public void AddProvider(ILoggerProvider provider)
        {
            this.loggerFactory.AddProvider(provider);
        }

        public ILogger CreateLogger(string categoryName)
        {
            var logger = this.loggerFactory.CreateLogger(categoryName);
            return new LoggerWrapper(logger, categoryName, this.hubName, this.workerId, this.providerFactory);
        }

        public void Dispose()
        {
            this.loggerFactory.Dispose();
        }

        class LoggerWrapper : ILogger
        {
            readonly ILogger logger;
            readonly string prefix;
            readonly string hubName;
            readonly NetheriteProviderFactory providerFactory;
            readonly bool fullTracing;

            public LoggerWrapper(ILogger logger, string category, string hubName, string workerId, NetheriteProviderFactory providerFactory)
            {
                this.logger = logger;
                this.prefix = $"{workerId} [{category}]";
                this.hubName = hubName;
                this.providerFactory = providerFactory;
                this.fullTracing = this.providerFactory.TraceToBlob || this.providerFactory.TraceToConsole;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return this.logger.BeginScope(state);
            }

            public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel)
            {
                return this.fullTracing || this.logger.IsEnabled(logLevel);
            }

            public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                if (this.IsEnabled(logLevel))
                {
                    this.logger.Log(logLevel, eventId, state, exception, formatter);

                    if (this.providerFactory.TraceToConsole || this.providerFactory.TraceToBlob)
                    {
                        string formattedString = $"{DateTime.UtcNow:o} {this.prefix}s{(int)logLevel} {formatter(state, exception)}";

                        if (this.providerFactory.TraceToConsole)
                        {
                            System.Console.WriteLine(formattedString);
                        }

                        NetheriteProviderFactory.BlobLogger?.WriteLine(formattedString);
                    }
                }
            }
        }

    }

}
