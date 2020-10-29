//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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
