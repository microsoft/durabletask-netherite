// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public enum Events
    {
        started,
        finished,
        failed,
        requesttimeout,
        deadlinetimeout,
        canceled,
        exception
    }
}
