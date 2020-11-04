// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    /// <summary>
    /// Schema for instance queries
    /// </summary>
    [DataContract]
    public class InstanceQuery
    {
        /// <summary>
        /// The subset of runtime statuses to return, or null if all
        /// </summary>
        [DataMember]
        public OrchestrationStatus[] RuntimeStatus { get; set; }

        /// <summary>
        /// The lowest creation time to return, or null if no lower bound
        /// </summary>
        [DataMember]
        public DateTime? CreatedTimeFrom { get; set; }

        /// <summary>
        /// The latest creation time to return, or null if no upper bound
        /// </summary>
        [DataMember]
        public DateTime? CreatedTimeTo { get; set; }

        /// <summary>
        /// A prefix of the instance ids to return, or null if no specific prefix
        /// </summary>
        [DataMember]
        public string InstanceIdPrefix { get; set; }

        /// <summary>
        /// Whether to fetch the input along with the orchestration state.
        /// </summary>
        [DataMember]
        public bool FetchInput { get; set; }

        /// <summary>
        /// Whether to prefetch the history for all returned instances.
        /// </summary>
        [DataMember]
        internal bool PrefetchHistory { get; set; }


        /// <summary>
        /// Construct an instance query with the given parameters.
        /// </summary>
        /// <param name="runtimeStatus">The subset of runtime statuses to return, or null if all</param>
        /// <param name="createdTimeFrom">The lowest creation time to return, or null if no lower bound.</param>
        /// <param name="createdTimeTo">The latest creation time to return, or null if no upper bound.</param>
        /// <param name="instanceIdPrefix">A prefix of the instance ids to return, or null if no specific prefix.</param>
        /// <param name="fetchInput">Whether to fetch the input along with the orchestration state.</param>
        public InstanceQuery(
                    OrchestrationStatus[] runtimeStatus = null, 
                    DateTime? createdTimeFrom = null, 
                    DateTime? createdTimeTo = null,
                    string instanceIdPrefix = null, 
                    bool fetchInput = true)
        {
            this.RuntimeStatus = runtimeStatus;
            this.CreatedTimeFrom = createdTimeFrom;
            this.CreatedTimeTo = createdTimeTo;
            this.InstanceIdPrefix = instanceIdPrefix;
            this.FetchInput = fetchInput;
        }

        internal bool HasRuntimeStatus => this.RuntimeStatus != null && this.RuntimeStatus.Length > 0;

        internal bool IsSet => this.HasRuntimeStatus || !string.IsNullOrWhiteSpace(this.InstanceIdPrefix)
                                    || !(this.CreatedTimeFrom is null) || !(this.CreatedTimeTo is null);

        internal bool Matches(OrchestrationState targetState)
        {
            if (targetState == null)
                throw new ArgumentNullException(nameof(targetState));

            return (!this.HasRuntimeStatus || this.RuntimeStatus.Contains(targetState.OrchestrationStatus))
                     && (string.IsNullOrWhiteSpace(this.InstanceIdPrefix) || targetState.OrchestrationInstance.InstanceId.StartsWith(this.InstanceIdPrefix))
                     && (!this.CreatedTimeFrom.HasValue || targetState.CreatedTime >= this.CreatedTimeFrom.Value)
                     && (!this.CreatedTimeTo.HasValue || targetState.CreatedTime <= this.CreatedTimeTo.Value);
        }
    }
}
