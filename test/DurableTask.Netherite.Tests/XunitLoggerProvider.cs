// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Tests
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Xunit.Abstractions;

    public class XunitLoggerProvider : ILoggerProvider
    {
        public ITestOutputHelper Output { get; set; }

        public XunitLoggerProvider(ITestOutputHelper testOutputHelper = null)
        {
            this.Output = testOutputHelper;
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

            public bool IsEnabled(LogLevel logLevel) => TestHelpers.UnitTestLogLevel <= logLevel;

            public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                // Write the information to the system trace
                string formattedString = formatter(state, exception);

                lock (this.provider)
                {
                    switch (logLevel)
                    {
                        case LogLevel.Information:
                        case LogLevel.Debug:
                        case LogLevel.Trace:
                            System.Diagnostics.Trace.TraceInformation($"{formattedString}");
                            break;
                        case LogLevel.Error:
                        case LogLevel.Critical:
                            System.Diagnostics.Trace.TraceError($"{formattedString}");
                            if (exception != null)
                                System.Diagnostics.Trace.TraceError(exception.ToString());
                            break;
                        case LogLevel.Warning:
                            System.Diagnostics.Trace.TraceWarning($"{formattedString}");
                            if (exception != null)
                                System.Diagnostics.Trace.TraceWarning(exception.ToString());
                            break;
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
