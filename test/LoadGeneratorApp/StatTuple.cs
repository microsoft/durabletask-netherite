// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;

    public struct StatTuple
    {
        public JToken Result;
        public DateTime? Time;
        public double? Duration;
    }
}
