// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Threading;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Azure;

    /// <summary>
    /// A simple utility class for writing text to an append blob in Azure Storage, using a periodic timer.
    /// For testing and debugging, not meant for production use.
    /// </summary>
    class BlobLogger
    {
        readonly DateTime starttime;
        readonly CloudAppendBlob blob;
        readonly object flushLock = new object();
        readonly object lineLock = new object();
        readonly ConcurrentQueue<MemoryStream> writebackQueue;
        MemoryStream memoryStream;
        StreamWriter writer;

#pragma warning disable IDE0052 // Cannot remove timer reference, otherwise timer is garbage-collected and stops
        readonly Timer timer;
#pragma warning restore IDE0052 

        public BlobLogger(ConnectionInfo storageConnection, string hubName, string workerId)
        {
            this.starttime = DateTime.UtcNow;

            CloudStorageAccount storageAccount = storageConnection.GetAzureStorageV11AccountAsync(CancellationToken.None).GetAwaiter().GetResult();
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("logs");
            container.CreateIfNotExists();
            this.blob = container.GetAppendBlobReference($"{hubName}.{workerId}.{this.starttime:o}.log");
            this.blob.CreateOrReplace();

            this.memoryStream = new MemoryStream();
            this.writer = new StreamWriter(this.memoryStream);
            this.writebackQueue = new ConcurrentQueue<MemoryStream>();

            int interval = 14000 + new Random().Next(1000);
            this.timer = new Timer(this.Flush, null, interval, interval);
        }

        public void WriteLine(string line)
        {
            lock (this.lineLock)
            {
                this.writer.WriteLine(line);

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
            this.writer = new StreamWriter(this.memoryStream);
        }

        public void Flush(object ignored)
        {
            if (Monitor.TryEnter(this.flushLock))
            {
                try
                {
                    lock (this.lineLock)
                    {
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