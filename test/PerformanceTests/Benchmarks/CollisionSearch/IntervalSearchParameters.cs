// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.CollisionSearch
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Linq;

    /// <summary>
    /// A simple microbenchmark orchestration that searches for hash collisions.
    /// </summary>

    public struct IntervalSearchParameters
    {
        // the hash code for which we want to find a collision
        public int Target { get; set; }
        // The start of the interval
        public long Start { get; set; }
        // The size of the interval
        public long Count { get; set; }
    }
}
