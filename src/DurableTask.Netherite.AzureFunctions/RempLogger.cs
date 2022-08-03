// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using DurableTask.Netherite.Tracing;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Azure;
    using Microsoft.Extensions.Logging;

    class RempLogger : RempFormat.IListener
    {
        readonly DateTime starttime;
        readonly CloudAppendBlob blob;
        readonly object flushLock = new();
        readonly object writerLock = new();
        readonly ConcurrentQueue<MemoryStream> writebackQueue;
        MemoryStream memoryStream;
        RempWriter writer;

#pragma warning disable IDE0052 // Cannot remove timer reference, otherwise timer is garbage-collected and stops
        readonly Timer timer;
#pragma warning restore IDE0052 

        public RempLogger(string storageConnectionString, string hubName, string workerId)
        {
            this.starttime = DateTime.UtcNow;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("remp");
            container.CreateIfNotExists();
            this.blob = container.GetAppendBlobReference($"{hubName}.{workerId}.{this.starttime:o}.remp");
            this.blob.CreateOrReplace();

            this.memoryStream = new MemoryStream();
            this.writer = new RempWriter(this.memoryStream);
            this.writebackQueue = new ConcurrentQueue<MemoryStream>();

            int interval = 14000 + new Random().Next(1000);
            this.timer = new Timer(this.Flush, null, interval, interval);
        }

        public void WorkerHeader(string workerId, IEnumerable<RempFormat.WorkitemGroup> groups)
        {
            lock (this.writerLock)
            {
                this.writer.WorkerHeader(workerId, groups);

                if (this.memoryStream.Position > 7.8 * 1024 * 1024)
                {
                    this.AddBufferToWritebackQueue();
                }
            }
        }

        public void WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<RempFormat.NamedPayload> consumedMessages, IEnumerable<RempFormat.NamedPayload> producedMessages, bool allowSpeculation, RempFormat.InstanceState? instanceState)
        {
            lock (this.writerLock)
            {
                this.writer.WorkItem(timeStamp, workItemId, group, latencyMs, consumedMessages, producedMessages, allowSpeculation, instanceState);

                if (this.memoryStream.Position > 7.8 * 1024 * 1024)
                {
                    this.AddBufferToWritebackQueue();
                }
            }
        }

        void AddBufferToWritebackQueue()
        {
            // grab current buffer and create new one
            this.writer.Flush();
            this.writebackQueue.Enqueue(this.memoryStream);
            this.memoryStream = new MemoryStream();
            this.writer = new RempWriter(this.memoryStream);
        }

        public void Flush(object ignored)
        {
            if (Monitor.TryEnter(this.flushLock))
            {
                try
                {
                    lock (this.writerLock)
                    {
                        this.writer.Flush();

                        if (this.memoryStream.Position > 0)
                        {
                            this.AddBufferToWritebackQueue();
                        }
                    }

                    while (this.writebackQueue.TryDequeue(out MemoryStream toSave))
                    {
                        // save to storage
                        toSave.Seek(0, SeekOrigin.Begin);
                        this.blob.AppendFromStream(toSave);
                        toSave.Dispose();
                    }
                }
                finally
                {
                    Monitor.Exit(this.flushLock);
                }
            }
        }

      
    }
}
#endif