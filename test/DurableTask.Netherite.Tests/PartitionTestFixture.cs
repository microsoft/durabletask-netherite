// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using Xunit.Abstractions;

    public class PartitionTestFixture : IDisposable
    {
        readonly TestTraceListener traceListener;
        readonly XunitLoggerProvider loggerProvider;

        internal TestOrchestrationHost Host { get; private set; }
        internal ILoggerFactory LoggerFactory { get; private set; }

        public PartitionTestFixture()
        {
            this.LoggerFactory = new LoggerFactory();
            this.loggerProvider = new XunitLoggerProvider();
            this.LoggerFactory.AddProvider(this.loggerProvider);
            TestConstants.ValidateEnvironment();
            this.Host = TestConstants.GetTestOrchestrationHost(this.LoggerFactory);
            this.Host.StartAsync().Wait();
            this.traceListener = new TestTraceListener();
            Trace.Listeners.Add(this.traceListener);
        }

        public void Dispose()
        {
            this.ClearOutput();
            this.Host.StopAsync(false).Wait();
            this.Host.Dispose();
            Trace.Listeners.Remove(this.traceListener);
        }

        public void SetOutput(Action<string> output)
        {
            this.loggerProvider.Output = output;
            this.traceListener.Output = output;
        }

        public void ClearOutput()
        {
            this.loggerProvider.Output = null;
            this.traceListener.Output = null;
        }

        internal class TestTraceListener : TraceListener
        {
            public Action<string> Output { get; set; }
            public override void Write(string message) {  }
            public override void WriteLine(string message) { this.Output?.Invoke($"{DateTime.Now:o} {message}"); }
        }
    }
}
