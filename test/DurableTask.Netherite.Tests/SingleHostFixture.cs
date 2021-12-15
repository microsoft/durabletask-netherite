// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using DurableTask.Netherite.Faster;
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
        readonly CacheDebugger cacheDebugger;

        Action<string> output;
        internal TestOrchestrationHost Host { get; private set; }
        internal ILoggerFactory LoggerFactory { get; private set; }

        string firstError;

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
            this.cacheDebugger = settings.CacheDebugger = new Faster.CacheDebugger();
            this.cacheDebugger.OnError += (message) =>
            {
                this.loggerProvider.Output?.Invoke($"CACHEDEBUGGER: {message}");
                this.traceListener.Output?.Invoke($"CACHEDEBUGGER: {message}");
                this.firstError ??= message;
            };
        }

        public void DumpCacheDebugger()
        {
            foreach (var line in this.cacheDebugger.Dump())
            {
                this.output?.Invoke(line);
            }
        }

        public void Dispose()
        {
            this.Host.StopAsync(false).Wait();
            this.Host.Dispose();
            Trace.Listeners.Remove(this.traceListener);
        }

        public bool HasError(out string error)
        {
            error = this.firstError;
            return error != null;
        }

        public void SetOutput(Action<string> output)
        {
            this.output = output;
            this.loggerProvider.Output = output;
            this.traceListener.Output = output;
        }

        internal class TestTraceListener : TraceListener
        {
            public Action<string> Output { get; set; }
            public override void Write(string message) {  }
            public override void WriteLine(string message) { this.Output?.Invoke($"{DateTime.Now:o} {message}"); }
        }
    }
}
