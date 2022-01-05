// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    [Serializable]
    public class FixedRateParameters : BaseParameters
    {
        /// <summary>
        /// Number of robots
        /// </summary>
        public int Robots { get; set; }

        /// <summary>
        /// Number of requests issued by each robot each second
        /// </summary>
        public double Rate { get; set; }

        /// <summary>
        /// Duration of the test, in number seconds
        /// </summary>
        public int Duration { get; set; }

        /// <summary>
        /// When to start collecting results (in seconds, after start of test)
        /// </summary>
        public int Warmup { get; set; }

        /// <summary>
        /// When to stop collecting results (in seconds, before end of test)
        /// </summary>
        public int Cooldown { get; set; }

        // a short human-readable string describing the test, used for filenames and in display
        public override string ToString()
        {
            return string.Format($"{base.ToString()}-{this.Rate}rate-{this.Robots}robots-{this.Duration}sec");
        }
    }
}
