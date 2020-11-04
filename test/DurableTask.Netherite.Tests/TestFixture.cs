// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Tests
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Xunit.Abstractions;

    public class TestFixture : IDisposable
    {
        public TestFixture()
        {
            this.LoggerFactory = new LoggerFactory();
            this.LoggerProvider = new XunitLoggerProvider();
            this.LoggerFactory.AddProvider(this.LoggerProvider);
            this.Host = TestHelpers.GetTestOrchestrationHost(this.LoggerFactory);
            this.Host.StartAsync().Wait();
        }

        public void Dispose()
        {
            this.LoggerProvider.Output = null;
            this.Host.StopAsync(false).Wait();
            this.Host.Dispose();
        }

        internal TestOrchestrationHost Host { get; private set; }

        internal XunitLoggerProvider LoggerProvider { get; private set; }

        internal ILoggerFactory LoggerFactory { get; private set; }

    }
}
