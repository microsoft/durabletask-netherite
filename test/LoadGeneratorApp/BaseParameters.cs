// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public class BaseParameters
    {
        /// <summary>
        /// A tag to prefix tests with.
        /// </summary>
        public string Prefix { get; set; }

        /// <summary>
        /// Which operation should be used for the benchmark
        /// </summary>
        public Operations Operation { get; set; }

        /// <summary>
        /// The number of objects (counters, rows, accounts, ...) over which the
        /// requests get dispersed
        /// </summary>
        public int NumberObjects { get; set; } = 1000;

        /// <summary>
        /// Timeout setting for requests
        /// </summary>
        public int TimeoutSeconds { get; set; } = 60;

        /// <summary>
        /// the random seed for this scenario
        /// </summary>
        public int RandomSeed { get; set; }

        /// <summary>
        /// A connection string to use for direct calls to EH
        /// </summary>
        public string EventHubsConnection { get; set; }

        /// <summary>
        /// Choose the service URLs to use (or leave empty for default)
        /// </summary>
        public string ServiceUrls { get; set; }

        /// <summary>
        /// The URL of the generator, used for callbacks
        /// </summary>
        public string GeneratorUrl { get; set; }


        public override string ToString()
        {
            return string.Format($"{Prefix}-{Operation}-{RandomSeed}seed");
        }
    }
}
