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

    /// <summary>
    /// A test fixture that starts the host before the tests start, and shuts it down after all the tests complete.
    /// </summary>
    public class SingleHostFixture : IDisposable
    {
        readonly TestTraceListener traceListener;
        readonly XunitLoggerProvider loggerProvider;
        internal TestOrchestrationHost Host { get; private set; }
        internal ILoggerFactory LoggerFactory { get; private set; }

        public SingleHostFixture()
        {
            this.LoggerFactory = new LoggerFactory();
            this.loggerProvider = new XunitLoggerProvider();
            this.LoggerFactory.AddProvider(this.loggerProvider);
            TestConstants.ValidateEnvironment();
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.PartitionManagement = PartitionManagementOptions.EventProcessorHost;
            this.Host = new TestOrchestrationHost(settings, this.LoggerFactory);
            this.Host.StartAsync().Wait();
            this.traceListener = new TestTraceListener();
            Trace.Listeners.Add(this.traceListener);
            var cacheDebugger = settings.CacheDebugger = new Faster.CacheDebugger();
            cacheDebugger.OnError += (message) =>
            {
                this.loggerProvider.Output?.Invoke($"CACHEDEBUGGER: {message}");
                this.traceListener.Output?.Invoke($"CACHEDEBUGGER: {message}");
            };
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
