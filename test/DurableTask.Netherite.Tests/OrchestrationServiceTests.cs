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
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Extensions.Logging;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    public class OrchestrationServiceTests
    {
        readonly ILoggerFactory loggerFactory;

        public OrchestrationServiceTests(ITestOutputHelper outputHelper)
        {
            this.loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider(outputHelper);
            this.loggerFactory.AddProvider(loggerProvider);
        }

        [Fact]
        public async Task StopAsync_IsIdempotent()
        {
            int numStops = 3;
            IOrchestrationService service = TestHelpers.GetTestOrchestrationService(this.loggerFactory);
            for (int i =0; i < numStops; i++)
            {
                await service.StopAsync();
            }
        }

        [Fact]
        public async Task UnstartedService_CanBeSafelyStopped()
        {
            IOrchestrationService service = TestHelpers.GetTestOrchestrationService(this.loggerFactory);
            await service.StopAsync();
        }
    }
}
