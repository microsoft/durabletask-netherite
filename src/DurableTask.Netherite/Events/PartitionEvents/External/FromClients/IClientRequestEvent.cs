// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;

    interface IClientRequestEvent
    {
        Guid ClientId { get; set; }

        long RequestId { get; set; }

        DateTime TimeoutUtc { get; set; }

        EventId EventId { get; }
    }
}