// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace ScalingTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Extensions.Logging;

    public class ConsoleLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName)
            => new Logger(categoryName);

        public void Dispose()
        { }

        public class Logger : ILogger
        {
            readonly string categoryName;

            public Logger(string categoryName)
            {
                this.categoryName = categoryName;
            }

            public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

            public bool IsEnabled(LogLevel logLevel) => LogLevel.Trace <= logLevel;

            public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                // Write the information to the system trace
                string formattedString = formatter(state, exception);

                switch (logLevel)
                {
                    case LogLevel.Information:
                    case LogLevel.Debug:
                    case LogLevel.Trace:
                        Console.WriteLine(formattedString);
                        break;
                    case LogLevel.Error:
                    case LogLevel.Critical:
                        Console.Error.WriteLine(formattedString);
                        if (exception != null)
                            Console.Error.WriteLine(exception.ToString());
                        break;
                    case LogLevel.Warning:
                        Console.Error.WriteLine(formattedString);
                        if (exception != null)
                            Console.Error.WriteLine(exception.ToString());
                        break;
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
