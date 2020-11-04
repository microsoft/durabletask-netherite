// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    /// <summary>
    /// Functionality for splitting large events into smaller ones, or the reverse.
    /// </summary>
    static class FragmentationAndReassembly
    {
        public interface IEventFragment
        {
            EventId OriginalEventId { get; }

            byte[] Bytes { get; }

            bool IsLast { get; }
        }

        public static List<IEventFragment> Fragment(ArraySegment<byte> segment, Event original, int maxFragmentSize)
        {
            if (segment.Count <= maxFragmentSize)
                throw new ArgumentException(nameof(segment), "segment must be larger than max fragment size");

            var list = new List<IEventFragment>();
            int offset = segment.Offset;
            int length = segment.Count;
            int count = 0;
            while (length > 0)
            {
                int portion = Math.Min(length, maxFragmentSize);
                if (original is ClientEvent clientEvent)
                {
                    list.Add(new ClientEventFragment()
                    {
                        ClientId = clientEvent.ClientId,
                        RequestId = clientEvent.RequestId,
                        OriginalEventId = original.EventId,
                        Bytes = new ArraySegment<byte>(segment.Array, offset, portion).ToArray(),
                        Fragment = count++,
                        IsLast = (portion == length),
                    });
                }
                else if (original is PartitionUpdateEvent partitionEvent)
                {
                    list.Add(new PartitionEventFragment()
                    {
                        PartitionId = partitionEvent.PartitionId,
                        OriginalEventId = original.EventId,
                        Bytes = new ArraySegment<byte>(segment.Array, offset, portion).ToArray(),
                        Fragment = count++,
                        IsLast = (portion == length),
                    });
                }
                offset += portion;
                length -= portion;
            }
            return list;
        }

        public static TEvent Reassemble<TEvent>(MemoryStream stream, IEventFragment lastFragment) where TEvent : Event
        {
            stream.Write(lastFragment.Bytes, 0, lastFragment.Bytes.Length);
            stream.Seek(0, SeekOrigin.Begin);
            Packet.Deserialize(stream, out TEvent evt, null);
            stream.Dispose();
            return evt;
        }

        public static TEvent Reassemble<TEvent>(IEnumerable<IEventFragment> earlierFragments, IEventFragment lastFragment) where TEvent: Event
        {
            using (var stream = new MemoryStream())
            {
                foreach (var x in earlierFragments)
                {
                    stream.Write(x.Bytes, 0, x.Bytes.Length);
                }
                stream.Write(lastFragment.Bytes, 0, lastFragment.Bytes.Length);
                stream.Seek(0, SeekOrigin.Begin);
                Packet.Deserialize(stream, out TEvent evt, null);
                return evt;
            }
        }
    }
}