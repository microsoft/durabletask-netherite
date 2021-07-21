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
            while (await channelReader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (channelReader.TryRead(out T item))
                {
                    yield return item;
                }
            }
        }
    }
}