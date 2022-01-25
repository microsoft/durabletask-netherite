// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Darq
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ISystemEmulator
    {
        void AddComponent(Component component);

        void Start(int randomSeed);
    }

    public interface IEvent
    {
    }

    public interface IContext 
    {
        // the current position in the history
        long HistoryPosition { get; }

        // signal a component
        void SendSignal(IEvent evt, string destination);

        // execute a local task asynchronously, and process the result event when complete
        void RunTask(Func<IEvent> task);
    }
    
    public abstract class Component
    {
        public string ComponentId { get; }

        public abstract void Process(IContext context, IEvent evt, long pos);
    }
}
