// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Channels;

    public static class ChannelReaderExtensions
    {
        public static async IAsyncEnumerable<T> ReadAllAsync<T>(
            this ChannelReader<T> channelReader, 
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await channelReader.WaitToReadAsync(cancellationToken))
            {
                while (channelReader.TryRead(out T item))
                {
                    yield return item;
                }
            }
        }


#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> enumerable)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            foreach (var item in enumerable)
            {
                yield return item;
            }
        }
    }
}