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
        public Faster.CacheDebugger CacheDebugger { get; set; }

        public Faster.ReplayChecker ReplayChecker { get; set; }

        public Faster.FaultInjector FaultInjector { get; set; }


        internal event Action<string> OnError;

        internal void Error(string source, string message)
        {
            if (System.Diagnostics.Debugger.IsAttached)
            {
                System.Diagnostics.Debugger.Break();
            }
            if (this.OnError != null)
            {
                this.OnError($"TestHook-{source} !!! {message}");
            }
            Console.Error.WriteLine($"TestHook-{source} !!! {message}");
            System.Diagnostics.Trace.TraceError($"TestHook-{source} !!! {message}");
        }

        public override string ToString()
        {
            return $"TestHooks:{(this.CacheDebugger != null ? " CacheDebugger" : "")}{(this.ReplayChecker != null ? " ReplayChecker" : "")}{(this.FaultInjector != null ? " FaultInjector" : "")}";
        }
    }
}
