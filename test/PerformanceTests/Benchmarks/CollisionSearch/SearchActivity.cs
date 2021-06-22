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
    /// An activity that searches an interval for hash inverses.
    /// </summary>
    public static class SearchActivity
    {
        public const long MaxIntervalSize = 1000000000;

        [FunctionName(nameof(SearchActivity))]
        public static Task<List<long>> Run([ActivityTrigger] IDurableActivityContext context)
        {
            var input = context.GetInput<IntervalSearchParameters>();

            if (input.Count > MaxIntervalSize)
            {
                throw new ArgumentOutOfRangeException("interval is too large");
            }

            var results = new List<long>();
            for (var i = input.Start; i < input.Start + input.Count; i++)
            {
                if (i.GetHashCode() == input.Target)
                    results.Add(i);
            }

            return Task.FromResult(results);
        }
    }
}
