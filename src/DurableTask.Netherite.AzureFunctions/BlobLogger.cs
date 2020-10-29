// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.IO;
    using System.Threading;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;

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
        MemoryStream memoryStream;
        StreamWriter writer;

#pragma warning disable IDE0052 // Cannot remove timer reference, otherwise timer is garbage-collected and stops
        readonly Timer timer;
#pragma warning restore IDE0052 

        public BlobLogger(string storageConnectionString, string workerId)
        {
            this.starttime = DateTime.UtcNow;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("logs");
            container.CreateIfNotExists();
            this.blob = container.GetAppendBlobReference($"{workerId}.{this.starttime:o}.log");
            this.blob.CreateOrReplace();

            this.memoryStream = new MemoryStream();
            this.writer = new StreamWriter(this.memoryStream);

            int interval = 14000 + new Random().Next(1000);
            this.timer = new Timer(this.Flush, null, interval, interval);
        }

        public void WriteLine(string line)
        {
            lock (this.lineLock)
            {
                this.writer.WriteLine(line);
            }
        }

        public void Flush(object ignored)
        {
            if (Monitor.TryEnter(this.flushLock))
            {
                try
                {
                    MemoryStream toSave = null;

                    // grab current buffer and create new one
                    lock (this.lineLock)
                    {
                        this.writer.Flush();
                        if (this.memoryStream.Position > 0)
                        {
                            toSave = this.memoryStream;
                            this.memoryStream = new MemoryStream();
                            this.writer = new StreamWriter(this.memoryStream);
                        }
                    }

                    if (toSave != null)
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
