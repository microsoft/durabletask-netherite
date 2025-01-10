// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Data.Tables;
    using DurableTask.Core;
    using DurableTask.Netherite.Faster;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json.Serialization;
    using Xunit;
    using Xunit.Abstractions;
    using static DurableTask.Netherite.Tests.ScenarioTests;

    [Collection("NetheriteTests")]
    [Trait("AnyTransport", "false")]
    public class LoadPublisherTests
    {
        string tableName;
        readonly List<string> taskHubs = new List<string>();

        IStorageLayer GetFreshTaskHub(string tableName)
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings("SingleHost");
            this.tableName = settings.LoadInformationAzureTableName = tableName;
            settings.HubName = $"{nameof(LoadPublisherTests)}-{Guid.NewGuid()}";
            this.taskHubs.Add(settings.HubName);
            var loggerFactory = new LoggerFactory();
            return new FasterStorageLayer(settings, new OrchestrationServiceTraceHelper(loggerFactory, LogLevel.None, "test", settings.HubName), loggerFactory);
        }

        async Task NothingLeftBehind()
        {
            if (this.tableName == null)
            {
                foreach (var taskHub in this.taskHubs)
                {
                    // there should not be anything left inside the blob container
                    var blobContainerName = taskHub.ToLowerInvariant() + "-storage";
                    var blobServiceClient = new Azure.Storage.Blobs.BlobServiceClient(Environment.GetEnvironmentVariable(TestConstants.StorageConnectionName));
                    var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
                    if (await blobContainerClient.ExistsAsync())
                    {
                        var allBlobs = await blobContainerClient.GetBlobsAsync().ToListAsync();
                        Assert.Empty(allBlobs);
                    }
                }
            }
            else
            {
                // there should not be anything left in the table
                var tableClient = new TableClient(Environment.GetEnvironmentVariable(TestConstants.StorageConnectionName), this.tableName);
                try
                {
                    await foreach (var e in tableClient.QueryAsync<TableEntity>())
                    {
                        Assert.Fail("table is not empty");
                    }
                }
                catch (Azure.RequestFailedException e) when (e.Status == 404) // table may not exist
                {
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IdempotentCreationAndDeletion(bool useBlobs)
        {
            string tableName = $"Test{Guid.NewGuid():N}";
            IStorageLayer taskhub = this.GetFreshTaskHub(useBlobs ? null : tableName);

            // create taskhub and load publisher
            await taskhub.CreateTaskhubIfNotExistsAsync();

            var parameters = await taskhub.TryLoadTaskhubAsync(true);

            // this is superfluous since it is already implied by taskhub
            await taskhub.LoadPublisher.CreateIfNotExistsAsync(CancellationToken.None);
            await taskhub.LoadPublisher.DeleteIfExistsAsync(CancellationToken.None);

            // delete taskhub and load publisher
            await taskhub.DeleteTaskhubAsync();

            // check that nothing is left in storage
            await this.NothingLeftBehind();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeleteNonexisting(bool useBlobs)
        {
            IStorageLayer taskhub = this.GetFreshTaskHub(useBlobs ? null : "IDoNotExist");
            await taskhub.LoadPublisher.DeleteIfExistsAsync(CancellationToken.None); // should not throw

            // check that nothing is left in storage
            await this.NothingLeftBehind();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PopulateAndQuery(bool useBlobs)
        {
            IStorageLayer taskhub = this.GetFreshTaskHub(useBlobs ? null : $"Test{Guid.NewGuid():N}");

            // create taskhub and load publisher
            await taskhub.CreateTaskhubIfNotExistsAsync();

            // publish empty
            await taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>(), CancellationToken.None);

            // publish two
            await taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { {0, this.Create("A") }, {1, this.Create("B") } }, CancellationToken.None);

            // publish another two
            await taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { {0, this.Create("C") }, {2, this.Create("D") } }, CancellationToken.None);

            // cancel a publishing
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { { 1, this.Create("Y") }, { 2, this.Create("Z") } }, new CancellationToken(true)));

            // do conflicting publishes
            Parallel.For(0, 10, i =>
            {
                taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { { 3, this.Create("E") } }, CancellationToken.None).Wait();
            });

            // read all
            var results = await taskhub.LoadPublisher.QueryAsync(CancellationToken.None);

            this.Check(results[0], "C");
            this.Check(results[1], "B");
            this.Check(results[2], "D");
            this.Check(results[3], "E");

            // cancel a read
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => taskhub.LoadPublisher.QueryAsync(new CancellationToken(true)));

            // delete taskhub and load publisher
            await taskhub.DeleteTaskhubAsync();

            // check that nothing is left in storage
            await this.NothingLeftBehind();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeleteOne(bool useBlobs)
        {
            string tableName = useBlobs ? null : $"Test{Guid.NewGuid():N}";

            IStorageLayer taskhub1 = this.GetFreshTaskHub(tableName);
            IStorageLayer taskhub2 = this.GetFreshTaskHub(tableName);

            // create taskhubs and load publishers
            await Task.WhenAll(taskhub1.CreateTaskhubIfNotExistsAsync(), taskhub2.CreateTaskhubIfNotExistsAsync());

            // publish two
            await Task.WhenAll(
                taskhub1.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { { 0, this.Create("A") }, { 1, this.Create("B") } }, CancellationToken.None),
                taskhub2.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { { 0, this.Create("A") }, { 1, this.Create("B") } }, CancellationToken.None));

            // delete one 
            await taskhub1.DeleteTaskhubAsync();

            // check that stuff was deleted
            var queryResults = await taskhub1.LoadPublisher.QueryAsync(CancellationToken.None);
            Assert.Empty(queryResults);

            // check that the other task hub is still there
            var results = await taskhub2.LoadPublisher.QueryAsync(CancellationToken.None);
            this.Check(results[0], "A");
            this.Check(results[1], "B");

            await taskhub2.DeleteTaskhubAsync();

            // check that nothing is left in storage
            await this.NothingLeftBehind();
        }

        PartitionLoadInfo Create(string worker)
        {
            return new PartitionLoadInfo()
            {
                WorkerId = worker,
                Activities = 11,
                CacheMB = 1.1,
                CachePct = 33,
                CommitLogPosition = 64,
                InputQueuePosition = 1231,
                Instances = 3,
                LatencyTrend = "IIIII",
                MissRate = 0.1,
                Outbox = 44,
                Requests = 55,
                Timers = 66,
                Wakeup = DateTime.Parse("2022-10-08T17:00:44.7400082Z").ToUniversalTime(),
                WorkItems = 77
            };
        }

        void Check(PartitionLoadInfo actual, string worker)
        {
            var expected = this.Create(worker);
            Assert.Equal(expected.WorkerId, actual.WorkerId);
            Assert.Equal(expected.Activities, actual.Activities);
            Assert.Equal(expected.CacheMB, actual.CacheMB);
            Assert.Equal(expected.CachePct, actual.CachePct);
            Assert.Equal(expected.CommitLogPosition, actual.CommitLogPosition);
            Assert.Equal(expected.InputQueuePosition, actual.InputQueuePosition);
            Assert.Equal(expected.Instances, actual.Instances);
            Assert.Equal(expected.LatencyTrend, actual.LatencyTrend);
            Assert.Equal(expected.MissRate, actual.MissRate);
            Assert.Equal(expected.Outbox, actual.Outbox);
            Assert.Equal(expected.Requests, actual.Requests);
            Assert.Equal(expected.Timers, actual.Timers);
            Assert.Equal(expected.Wakeup, actual.Wakeup);
            Assert.Equal(expected.WorkItems, actual.WorkItems);
            Assert.Equal(expected.WorkItems, actual.WorkItems);
        }

        [Fact]
        public async Task PublishToMissingTable()
        {
            IStorageLayer taskhub = this.GetFreshTaskHub("IDoNotExist");
            await Assert.ThrowsAnyAsync<Azure.RequestFailedException>(() => taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { { 1, this.Create("Y") } }, CancellationToken.None));
        }

        [Fact]
        public async Task PublishToMissingTaskHub()
        {
            IStorageLayer taskhub = this.GetFreshTaskHub(null);
            await Assert.ThrowsAnyAsync<Azure.RequestFailedException>(() => taskhub.LoadPublisher.PublishAsync(new Dictionary<uint, PartitionLoadInfo>() { { 1, this.Create("Y") } }, CancellationToken.None));
        }

        [Fact]
        public async Task QueryFromMissingTable()
        {
            IStorageLayer taskhub = this.GetFreshTaskHub("IDoNotExist");
            await Assert.ThrowsAnyAsync<Azure.RequestFailedException>(() => taskhub.LoadPublisher.QueryAsync(CancellationToken.None));
        }

        [Fact]
        public async Task QueryFromMissingTaskHub()
        {
            IStorageLayer taskhub = this.GetFreshTaskHub(null);
            await Assert.ThrowsAnyAsync<Azure.RequestFailedException>(() => taskhub.LoadPublisher.QueryAsync(CancellationToken.None));
        }
    }
}
