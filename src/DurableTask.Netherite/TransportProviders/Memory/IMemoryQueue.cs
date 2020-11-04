// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
