// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Xunit.Abstractions;

    public class XunitLoggerProvider : ILoggerProvider
    {
        public XunitLoggerProvider()
        {
        }

        public ILogger CreateLogger(string categoryName)
            => new XunitLogger(this, categoryName);

        public void Dispose()
        { }

        public class XunitLogger : ILogger
        {
            readonly XunitLoggerProvider provider;
            readonly string categoryName;

            public XunitLogger(XunitLoggerProvider provider, string categoryName)
            {
                this.provider = provider;
                this.categoryName = categoryName;
            }

            public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

            public bool IsEnabled(LogLevel logLevel) => TestConstants.UnitTestLogLevel <= logLevel;

            public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                // Write the information to the system trace
                string formattedString = formatter(state, exception);

                int attempts = 0;
                while (++attempts <= 2)
                {
                    try
                    {
                        switch (logLevel)
                        {
                            case LogLevel.Information:
                            case LogLevel.Debug:
                            case LogLevel.Trace:
                                System.Diagnostics.Trace.TraceInformation(formattedString);
                                break;
                            case LogLevel.Error:
                            case LogLevel.Critical:
                                System.Diagnostics.Trace.TraceError(formattedString);
                                if (exception != null)
                                    System.Diagnostics.Trace.TraceError(exception.ToString());
                                break;
                            case LogLevel.Warning:
                                System.Diagnostics.Trace.TraceWarning(formattedString);
                                if (exception != null)
                                    System.Diagnostics.Trace.TraceWarning(exception.ToString());
                                break;
                        }
                    }
                    catch (InvalidOperationException) when (attempts < 2)
                    {
                        continue; // logger throws this sometimes when listener list is being concurrently modified
                    }
                }
            }

            class NoopDisposable : IDisposable
            {
                public static NoopDisposable Instance = new NoopDisposable();
                public void Dispose()
                { }
            }
        }
    }
}
