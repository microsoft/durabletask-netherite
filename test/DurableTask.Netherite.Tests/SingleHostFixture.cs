// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
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
            settings.PartitionManagement = PartitionManagementOptions.EventProcessorHost;
            if (useReplayChecker)
            {
                settings.TestHooks.ReplayChecker = new Faster.ReplayChecker(settings.TestHooks);
            }
            settings.TestHooks.OnError += (message) =>
            {
                System.Diagnostics.Trace.WriteLine($"TESTHOOKS: {message}");
                this.TestHooksError ??= message;
            };
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

        public void Dispose()
        {
            this.Host.StopAsync(false).Wait();
            this.Host.Dispose();
            Trace.Listeners.Remove(this.traceListener);
        }

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
