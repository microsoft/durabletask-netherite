// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public sealed class SimplestGetUserTask : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            return "Gabbar";
        }
    }

    public class SimplestGreetingsOrchestration : TaskOrchestration<string, string>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
            int delayInSeconds = string.IsNullOrWhiteSpace(input) ? 0 : Int32.Parse(input);

            if (delayInSeconds > 0)
            {
                await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(delayInSeconds)), null);
            }

            string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = greeting;

            return greeting;
        }
    }

    public sealed class SimplestSendGreetingTask : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string user)
        {
            return "Greeting send to " + user;
        }
    }

    public class FanOutOrchestration : TaskOrchestration<int, int[]>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static int Result;


        public override async Task<int> RunTask(OrchestrationContext context, int[] input)
        {
            Task<int>[] squareTasks = new Task<int>[input.Length];
            for (int i = 0; i < input.Length; i++)
            {
                squareTasks[i] = context.ScheduleTask<int>(typeof(SquareIntTask), input[i]);
            }
            int[] squares = await Task.WhenAll(squareTasks);
            Result = await context.ScheduleTask<int>(typeof(SumIntTask), squares);
            return Result;
        }
 
    }

    public sealed class SquareIntTask : TaskActivity<int, int>
    {
        protected override int Execute(TaskContext context, int numToSquare)
        {
            return numToSquare * numToSquare;
        }
    }

    public sealed class SumIntTask : TaskActivity<int[], long>
    {
        protected override long Execute(TaskContext context, int[] numsToAdd)
        {
            long sum = 0;
            foreach (int num in numsToAdd)
            {
                sum += num;
            }
            return sum;
        }
    }

    public sealed class ContinueAsNewThenTimerOrchestration : TaskOrchestration<string, int>
    {
        public override async Task<string> RunTask(OrchestrationContext context, int input)
        {
            if (input == 0)
            {
                context.ContinueAsNew(1);

                return "continue as new";
            }
            else if (input == 1)
            {
                await context.CreateTimer(context.CurrentUtcDateTime, 0);

                return "OK";
            }
            else
            {
                throw new InvalidOperationException();
            }
        }
    }

    [KnownType(typeof(EventConversationOrchestration.Responder))]
    public sealed class EventConversationOrchestration : TaskOrchestration<string, string>
    {
        readonly TaskCompletionSource<string> tcs
            = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

        // HACK: This is just a hack to communicate result of orchestration back to test
        public static bool OkResult;

        public async override Task<string> RunTask(OrchestrationContext context, string input)
        {
            // start a responder orchestration
            var responderId = "responderId" + context.OrchestrationInstance.InstanceId;
            var responderOrchestration = context.CreateSubOrchestrationInstance<string>(typeof(Responder), responderId, "Herkimer");

            // send the id of this orchestration to the responder
            var responderInstance = new OrchestrationInstance() { InstanceId = responderId };
            context.SendEvent(responderInstance, channelName, context.OrchestrationInstance.InstanceId);

            // wait for a response event 
            var message = await this.tcs.Task;
            if (message != "hi from Herkimer")
                throw new Exception("test failed");

            // tell the responder to stop listening, then wait for it to complete
            context.SendEvent(responderInstance, channelName, "stop");
            var receiverResult = await responderOrchestration;

            if (receiverResult != "Herkimer is done")
                throw new Exception("test failed");

            OkResult = true;

            return "OK";
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (name == channelName)
            {
                this.tcs.TrySetResult(input);
            }
        }

        const string channelName = "conversation";

        public class Responder : TaskOrchestration<string, string>
        {
            readonly TaskCompletionSource<string> tcs
                = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

            public async override Task<string> RunTask(OrchestrationContext context, string input)
            {
                var message = await this.tcs.Task;

                if (message == "stop")
                {
                    return $"{input} is done";
                }
                else
                {
                    // send a message back to the sender
                    var senderInstance = new OrchestrationInstance() { InstanceId = message };
                    context.SendEvent(senderInstance, channelName, $"hi from {input}");

                    // start over to wait for the next message
                    context.ContinueAsNew(input);

                    return "this value is meaningless";
                }
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                if (name == channelName)
                {
                    this.tcs.TrySetResult(input);
                }
            }
        }
    }

}
