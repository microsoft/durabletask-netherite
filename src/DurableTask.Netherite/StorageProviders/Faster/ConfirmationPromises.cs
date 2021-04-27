// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    class ConfirmationPromises
    {
        class PartitionInfo
        {
            public long LastConfirmed { get; set; } = 0;

            public SortedList<(long, int), TaskCompletionSource<object>> Promises { get; }
               = new SortedList<(long, int), TaskCompletionSource<object>>();
        }

        readonly Dictionary<uint, PartitionInfo> PartitionInfos = new Dictionary<uint, PartitionInfo>();

        public void CreateConfirmationPromise(TaskMessagesReceived evt)
        {
            var originPartition = evt.OriginPartition;

            if (!this.PartitionInfos.TryGetValue(originPartition, out var info))
            {
                this.PartitionInfos[originPartition] = info = new PartitionInfo();
            }

            if (evt.OriginPosition > info.LastConfirmed)
            {
                evt.ConfirmationPromise = new TaskCompletionSource<object>();

                info.Promises.TryAdd(evt.DedupPosition, evt.ConfirmationPromise);
            }
        }

        public void ResolveConfirmationPromises(uint partition, long position)
        {

            if (!this.PartitionInfos.TryGetValue(partition, out var info))
            {
                this.PartitionInfos[partition] = info = new PartitionInfo();
            }

            info.LastConfirmed = position;

            while (info.Promises.Count > 0 && info.Promises.First().Key.Item1 <= position)
            {
                var first = info.Promises.First();
                if (first.Key.Item1 > position)
                {
                    break;
                }
                else
                {
                    first.Value.TrySetResult(null);
                    info.Promises.RemoveAt(0);
                }
            }
        }
    }
}