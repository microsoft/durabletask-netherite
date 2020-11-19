// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    interface IRequiresPrefetch
    {
        public IEnumerable<TrackedObjectKey> KeysToPrefetch { get; }
    }
}
