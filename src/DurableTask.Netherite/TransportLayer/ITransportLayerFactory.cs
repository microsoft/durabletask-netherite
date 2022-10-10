// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{ 
    using System;
    using System.Collections.Generic;
    using System.Text;

    public interface ITransportLayerFactory
    {
        ITransportLayer Create(NetheriteOrchestrationService orchestrationService);
    }
}
