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
    using System.Threading.Tasks;
    using Xunit.Abstractions;

    /// <summary>
    /// A test fixture that starts the host before the tests start, and shuts it down after all the tests complete.
    /// </summary>
    public class SingleHostFixture : IDisposable
    {
        readonly TestTraceListener traceListener;
        readonly XunitLoggerProvider loggerProvider;
        readonly CacheDebugger cacheDebugger;

        internal TestOrchestrationHost Host { get; private set; }
        internal ILoggerFactory LoggerFactory { get; private set; }

        internal string TestHooksError { get; private set; }

        public SingleHostFixture()
            : this(TestConstants.GetNetheriteOrchestrationServiceSettings(), true, null)
        {
            this.Host.StartAsync().Wait();
        }

        SingleHostFixture(NetheriteOrchestrationServiceSettings settings, bool useReplayChecker, Action<string> output)
        {
            this.LoggerFactory = new LoggerFactory();
            this.loggerProvider = new XunitLoggerProvider();
            this.LoggerFactory.AddProvider(this.loggerProvider);
            this.traceListener = new TestTraceListener() { Output = output };
            Trace.Listeners.Add(this.traceListener);
            TestConstants.ValidateEnvironment();
            string timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff");
            settings.HubName = $"SingleHostFixture-{timestamp}";
            settings.PartitionManagement = PartitionManagementOptions.EventProcessorHost;
            this.cacheDebugger = settings.TestHooks.CacheDebugger = new Faster.CacheDebugger(settings.TestHooks);
            if (useReplayChecker)
            {
                settings.TestHooks.ReplayChecker = new Faster.ReplayChecker(settings.TestHooks);
            }
            settings.TestHooks.OnError += (message) =>
            {
                Trace.WriteLine($"TESTHOOKS: {message}");
                this.TestHooksError ??= message;
            };
            // start the host
            this.Host = new TestOrchestrationHost(settings, this.LoggerFactory);
        }

        public static async Task<SingleHostFixture> StartNew(NetheriteOrchestrationServiceSettings settings, bool useReplayChecker, TimeSpan timeout, Action<string> output)
        {
            var fixture = new SingleHostFixture(settings, useReplayChecker, output);
            var startupTask = fixture.Host.StartAsync(); 
            timeout = TestOrchestrationClient.AdjustTimeout(timeout);
            var timeoutTask = Task.Delay(timeout);
            await Task.WhenAny(timeoutTask, startupTask);
            if (!startupTask.IsCompleted)
            {
                throw new TimeoutException($"SingleHostFixture.StartNew timed out after {timeout}");
            }
            await startupTask;
            return fixture;
        }

        public void DumpCacheDebugger()
        {
            foreach (var line in this.cacheDebugger.Dump())
            {
                Trace.WriteLine(line);
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
            error = this.TestHooksError;
            return error != null;
        }

        // called before a new test, to route output to the test output
        public void SetOutput(Action<string> output)
        {
            this.traceListener.Output = output;
            this.TestHooksError = null;
        }

        internal class TestTraceListener : TraceListener
        {
            public Action<string> Output { get; set; }
            public override void Write(string message) {  }
            public override void WriteLine(string message) { this.Output?.Invoke($"{DateTime.Now:o} {message}"); }
        }
    }
}
