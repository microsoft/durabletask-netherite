// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Darq
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IEvent
    {
    }

    public interface IContext
    {
        // deterministic signal to some component
        void SignalAsync(IEvent evt, string destination);

        // nondeterministic signal to self
        void SignalSelfAsync(IEvent evt);
    }

    public abstract class Component
    {
        public string ComponentId { get; }
        public abstract void Process(IContext context, IEvent evt, long pos);
    }
}
