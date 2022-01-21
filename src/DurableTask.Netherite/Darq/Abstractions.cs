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
        long Id { get; }
        string Destination { get; }
    }

    public interface IRequestEvent<TResult> : IEvent
    {
    }

    public interface IContext
    {
        ValueTask SignalAsync(IEvent evt);
        ValueTask SignalSelfAsync(IEvent evt);
        ValueTask<TResult> CallAsync<TResult>(IRequestEvent<TResult> evt);
    }

    public abstract class Component
     {
        public abstract void Process(Tin inEvent, IContext<Tin,Tout> context);
    }
}
