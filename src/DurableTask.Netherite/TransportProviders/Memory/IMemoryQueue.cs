// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Emulated
{
    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    interface IMemoryQueue<T>
    {
        void Send(T evt);

        void Resume();

        long FirstInputQueuePosition { set; }
    }
}
