// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Text;

    /// <summary>
    /// Hooks for attaching additional checkers and debuggers during testing.
    /// </summary>
    public class TestHooks
    {
        public Faster.CacheDebugger CacheDebugger { get; set; }

        public Faster.ReplayChecker ReplayChecker { get; set; }

        public Faster.FaultInjector FaultInjector { get; set; }

        public Faster.CheckpointInjector CheckpointInjector { get; set; }

        internal event Action<string> OnError;
        bool launchDebugger = false; // may set this to true when hunting down bugs locally

        public bool FaultInjectionActive => this.FaultInjector != null || (this.CheckpointInjector?.InjectFaultAfterCompaction == true);

        internal void Error(string source, string message)
        {
            if (System.Diagnostics.Debugger.IsAttached)
            {
                System.Diagnostics.Debugger.Break();
            }
            else if (this.launchDebugger)
            {
                this.launchDebugger = false; // don't launch another one if the user detaches
                System.Diagnostics.Debugger.Launch();
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
            StringBuilder sb = new StringBuilder();

            sb.Append("TestHooks:");
            
            if (this.CacheDebugger != null)
            {
                sb.Append(" CacheDebugger");
            }
            if (this.ReplayChecker != null)
            {
                sb.Append(" ReplayChecker");

            }
            if (this.FaultInjector != null)
            {
                sb.Append(" FaultInjector");
            }
            if (this.CheckpointInjector != null)
            {
                sb.Append(" CheckpointInjector");
            }

            return sb.ToString();
        }
    }
}
