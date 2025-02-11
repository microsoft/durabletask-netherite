﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using FASTER.core;

    class FasterLog
    {
        readonly BlobManager blobManager;
        readonly FASTER.core.FasterLog log;
        readonly CancellationToken terminationToken;

        public FasterLog(BlobManager blobManager, NetheriteOrchestrationServiceSettings settings)
        {
            this.blobManager = blobManager;
            var eventlogsettings = blobManager.GetEventLogSettings(settings.UseSeparatePageBlobStorage, settings.FasterTuningParameters);
            this.log = new FASTER.core.FasterLog(eventlogsettings);
            blobManager.PartitionErrorHandler.AddDisposeTask($"{nameof(FasterLog)}.{nameof(this.Dispose)}", TimeSpan.FromMinutes(2), this.Dispose);
            this.terminationToken = blobManager.PartitionErrorHandler.Token;
        }

        void Dispose()
        {
            this.blobManager.TraceHelper.FasterProgress("Disposing FasterLog");
            this.log.Dispose();

            this.blobManager.TraceHelper.FasterProgress("Disposing FasterLog Device");
            this.blobManager.EventLogDevice.Dispose();

            this.blobManager.TraceHelper.FasterProgress("Disposed FasterLog");
        }

        public long BeginAddress => this.log.BeginAddress;
        public long TailAddress => this.log.TailAddress;
        public long CommittedUntilAddress => this.log.CommittedUntilAddress;

        public long Enqueue(byte[] entry)
        {
            try
            {
                return this.log.Enqueue(entry);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public long Enqueue(ReadOnlySpan<byte> entry)
        {
            try
            {
                return this.log.Enqueue(entry);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public async ValueTask CommitAsync()
        {
            try
            {
                await this.log.CommitAsync(this.terminationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public FasterLogScanIterator Scan(long beginAddress, long endAddress)
        {
            // used during recovery only

            // we are not wrapping termination exceptions here, since we would also have to wrap the iterator.
            // instead we wrap the whole replay loop in the caller.
            return this.log.Scan(beginAddress, endAddress);
        }

        public void TruncateUntil(long beforeAddress)
        {
            this.log.TruncateUntil(beforeAddress);
        }
    }
}