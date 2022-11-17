// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;

    interface IPagedResponse
    {
        string ContinuationToken { get; }

        int Count { get; }
    }
}