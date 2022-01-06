// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;

    /// <summary>
    /// Hooks for attaching additional checkers and debuggers during testing.
    /// </summary>
    public class TestHooks
    {
        internal Faster.CacheDebugger CacheDebugger { get; set; }

        internal Faster.ReplayChecker ReplayChecker { get; set; }

        public Faster.FaultInjector FaultInjector { get; set; }


        internal event Action<string> OnError;

        internal void Error(string source, string message)
        {
            if (System.Diagnostics.Debugger.IsAttached)
            {
                System.Diagnostics.Debugger.Break();
            }
            this.OnError($"TestHook-{source} !!! {message}");
        }
    }
}
