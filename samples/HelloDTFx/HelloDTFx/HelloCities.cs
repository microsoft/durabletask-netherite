// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

 
using System;
using DurableTask.Core;

public class HelloSequence : TaskOrchestration<List<string>, string>
{
    public override async Task<List<string>> RunTask(OrchestrationContext context, string input)
    {
        var result = new List<string>
        {
            await context.ScheduleTask<string>(typeof(SayHello), "Tokyo"),
            await context.ScheduleTask<string>(typeof(SayHello), "Seattle"),
            await context.ScheduleTask<string>(typeof(SayHello), "London"),
        };

        return result;
    }
}

public class SayHello : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string input)
    {
        return $"Hello, {input}!";
    }
}

 