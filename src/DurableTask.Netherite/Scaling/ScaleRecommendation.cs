// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Scaling
{
    using System;

    /// <summary>
    /// Represents a scale recommendation for the task hub given the current performance metrics.
    /// </summary>
    public class ScaleRecommendation : EventArgs
    {
        /// <summary>
        /// Constructs a scale recommendation.
        /// </summary>
        /// <param name="scaleAction">The scale action.</param>
        /// <param name="keepWorkersAlive">Whether to keep workers alive.</param>
        /// <param name="reason">Text describing the reason for the recommendation.</param>
        public ScaleRecommendation(ScaleAction scaleAction, bool keepWorkersAlive, string reason)
        {
            this.Action = scaleAction;
            this.KeepWorkersAlive = keepWorkersAlive;
            this.Reason = reason;
        }

        /// <summary>
        /// Gets the recommended scale action for the current task hub.
        /// </summary>
        public ScaleAction Action { get; }

        /// <summary>
        /// Gets a recommendation about whether to keep existing task hub workers alive.
        /// </summary>
        public bool KeepWorkersAlive { get; }

        /// <summary>
        /// Gets text describing why a particular scale action was recommended.
        /// </summary>
        public string Reason { get; }

        /// <summary>
        /// Gets a string description of the current <see cref="ScaleRecommendation"/> object.
        /// </summary>
        /// <returns>A string description useful for diagnostics.</returns>
        public override string ToString()
        {
            return $"{nameof(this.Action)}: {this.Action}, {nameof(this.KeepWorkersAlive)}: {this.KeepWorkersAlive}, {nameof(this.Reason)}: {this.Reason}";
        }
    }
}