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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite;
    using DurableTask.Netherite.Faster;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Logging;
    using Xunit;
    using Xunit.Abstractions;

    [Collection("NetheriteTests")]
    public class FasterTests
    {
        readonly ILoggerFactory loggerFactory;

        public FasterTests(ITestOutputHelper outputHelper)
        {
            this.loggerFactory = new LoggerFactory();
            var loggerProvider = new XunitLoggerProvider(outputHelper);
            this.loggerFactory.AddProvider(loggerProvider);
        }

        [Theory]
        [InlineData(false, 3, 5)]
        [InlineData(false, 30, 50)]
        [InlineData(false, 300, 500)]
        [InlineData(true, 1, 10)]
        [InlineData(true, 30, 50)]
        [InlineData(true, 300, 500)]
        public async Task RecoverLog(bool useAzure, int numEntries, int maxBytesPerEntry)
        {
            List<byte[]> entries = new List<byte[]>();
            List<long> positions = new List<long>();

            var random = new Random(0);

            var taskHubName = useAzure ? "test-taskhub" : Guid.NewGuid().ToString("N");
            var account = useAzure ? CloudStorageAccount.Parse(TestHelpers.GetAzureStorageConnectionString()) : null;
            var logger = this.loggerFactory.CreateLogger("testlogger");

            await BlobManager.DeleteTaskhubStorageAsync(account, taskHubName);

            // first, commit some number of random entries to the log and record the commit positions
            {
                var blobManager = new BlobManager(
                    account, 
                    account,
                    taskHubName, 
                    logger, 
                    Microsoft.Extensions.Logging.LogLevel.Trace, 
                    0, 
                    new PartitionErrorHandler(0, logger, Microsoft.Extensions.Logging.LogLevel.Trace, "account", taskHubName));

                await blobManager.StartAsync();
                var log = new FasterLog(blobManager, new NetheriteOrchestrationServiceSettings());

                for (int i = 0; i < numEntries; i++)
                {
                    var bytes = new byte[1 + random.Next(maxBytesPerEntry)];
                    random.NextBytes(bytes);
                    entries.Add(bytes);
                    positions.Add(log.Enqueue(entries[i]));
                }
                await log.CommitAsync();

                await blobManager.StopAsync();
            }

            // then, read back all the entries, and compare position and content
            {
                var blobManager = new BlobManager(
                    account,
                    account,
                    taskHubName, 
                    logger, 
                    Microsoft.Extensions.Logging.LogLevel.Trace, 
                    0, 
                    new PartitionErrorHandler(0, logger, Microsoft.Extensions.Logging.LogLevel.Trace, "account", taskHubName));

                await blobManager.StartAsync();
                var log = new FasterLog(blobManager, new NetheriteOrchestrationServiceSettings());

                int iterationCount = 0;
                await Iterate(0, positions[positions.Count - 1]);

                async Task Iterate(long from, long to)
                {
                    using (var iter = log.Scan(from, to + 1))
                    {
                        byte[] result;
                        int entryLength;
                        long currentAddress;

                        while (true)
                        {
                            var next = iter.NextAddress;

                            while (!iter.GetNext(out result, out entryLength, out currentAddress))
                            {
                                if (currentAddress >= to)
                                {
                                    Assert.Equal(iterationCount, numEntries);
                                    return;
                                }
                                await iter.WaitAsync();
                            }

                            // process entry
                            Assert.Equal(positions[iterationCount], next);
                            var reference = entries[iterationCount];
                            Assert.Equal(reference.Length, entryLength);
                            for (int i = 0; i < entryLength; i++)
                            {
                                Assert.Equal(reference[i], result[i]);
                            }

                            iterationCount++;
                        }
                    }
                }

                await blobManager.StopAsync();
            }

            await BlobManager.DeleteTaskhubStorageAsync(account, taskHubName);
        }
    }
}
