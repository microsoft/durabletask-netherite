// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public enum Operations
    {
        Invalid,

        // these are "null" tests, just to test the framework,
        // and can also be used for compositions
        Wait5,
        Wait20,

        // calls the ping test which tests the connection
        Ping,

        // calls GET or POST (without any arguments) on the base URL
        Get,
        Post,

        // test the callback mechanism
        CallbackTest,

        // calls a single hellocity orchestration
        // measuring latency at different levels (reported / httpTrigger / client)
        Hello,
        HelloHttp,
        HelloClient,
        HelloAWS,
        Hello100,

        // different versions of the sequence microbenchmark
        TriggeredSequenceLong,
        TriggeredSequenceWork,
        TriggeredSequenceData,
        QueueSequenceLong,
        QueueSequenceWork,
        QueueSequenceData,
        OrchestratedSequenceLong,
        OrchestratedSequenceWork,
        OrchestratedSequenceData,
        OrchestratedSequenceLongHttp,
        OrchestratedSequenceWorkHttp,
        OrchestratedSequenceDataHttp,
        AwsTriggeredSequenceLong,
        AwsTriggeredSequenceWork,
        AwsTriggeredSequenceData,
        AwsOrchestratedSequenceLong,
        AwsOrchestratedSequenceWork,
        AwsOrchestratedSequenceData,

        // calls a single bank orchestration of length 1, with pairs from the given supply of pairs
        Bank,
        BankNoConflicts,
        BankNoConflictsHttp,

        // calls the image processing workflow
        ImageAWS,
        ImageDF,
    }
}
