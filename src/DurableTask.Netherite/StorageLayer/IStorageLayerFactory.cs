// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{ 
    using System;
    using System.Collections.Generic;
    using System.Text;

    interface IStorageLayerFactory
    {
        IStorageLayer Create(NetheriteOrchestrationService orchestrationService);
    }
}
