// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class Dispatcher
    {
        readonly BaseParameters parameters;
        readonly Random random;
        readonly Client client;
        int iteration;
        readonly int numRobots;
        readonly int robotNumber;
        readonly int privateIndex;
        readonly Func<Guid, string> callbackUri;

        internal Dispatcher(BaseParameters parameters, Random random, int numRobots, Client client, int robotNumber, int privateIndex, Func<Guid, string> callbackUri)
        {
            this.parameters = parameters;
            this.random = random;
            this.client = client;
            this.numRobots = numRobots;
            this.robotNumber = robotNumber;
            this.privateIndex = privateIndex;
            this.callbackUri = callbackUri;

            // TODO: for load ramp, use
            //    privateIndex = (loadRampContext.RequestLoopNumber * numRobots) + context.RobotNumber;
        }



        public async Task<StatTuple> Execute(Operations operation, ILogger logger)
        {
            int currentIteration = this.iteration++;

            switch (operation)
            {
                case Operations.Wait20:
                    await Task.Delay(20);
                    return default(StatTuple);

                case Operations.Wait5:
                    await Task.Delay(5);
                    return default(StatTuple);

                case Operations.Ping:
                    {
                        string url = this.client.BaseUrl() + "/ping";

                        string result = await this.client.HttpClient.GetStringAsync(url);

                        logger?.LogTrace(result);
                        
                        return default(StatTuple);
                    }

                case Operations.Get:
                    {
                        string url = this.client.BaseUrl();

                        string result = await this.client.HttpClient.GetStringAsync(url);

                        logger?.LogTrace(result);

                        return default(StatTuple);
                    }

                case Operations.Post:
                    {
                        string url = this.client.BaseUrl();

                        HttpResponseMessage response = await this.client.HttpClient.PostAsync(url, new StringContent(""));

                        string result = await response.Content.ReadAsStringAsync();

                        logger?.LogTrace(result);

                        return default(StatTuple);
                    }

                case Operations.Hello100:
                    {
                        string url = this.client.BaseUrl() + "/hellocities";

                        HttpResponseMessage response = await this.client.HttpClient.PostAsync(url, new StringContent("100"));

                        string result = await response.Content.ReadAsStringAsync();

                        logger?.LogTrace(result);

                        var json = JObject.Parse(result);

                        if (json.TryGetValue("elapsedSeconds", out JToken value))
                        {
                            return new StatTuple() { Duration = 1000 * double.Parse((string)value) };
                        }
                        else
                        {
                            throw new TimeoutException();
                        }
                    }

                case Operations.Hello:
                case Operations.HelloHttp:
                case Operations.HelloClient:
                    {
                        bool useReportedLatency = (operation == Operations.Hello) && string.IsNullOrEmpty(this.parameters.EventHubsConnection);
                        bool measureClientLatency = (operation == Operations.HelloClient);

                        string orchestrationName = "HelloSequence3";
                        string instanceId = Guid.NewGuid().ToString("N");
                        object input = null;

                        logger?.LogTrace($"issued orchestration name={orchestrationName} instanceId={instanceId}");
                        
                        Stopwatch stopwatch = new Stopwatch();
                        stopwatch.Start();

                        var tuple = await this.client.RunOrchestrationAsync(logger, orchestrationName, instanceId, input, useReportedLatency).ConfigureAwait(false);

                        stopwatch.Stop();

                        logger?.LogTrace($"orchestration completed id={instanceId} result={tuple.Result}");                 

                        if (measureClientLatency)
                        {
                            tuple.Time = DateTime.UtcNow;
                            tuple.Duration = stopwatch.Elapsed.TotalMilliseconds;
                        }

                        return tuple;
                    }

                case Operations.OrchestratedSequenceLong:
                case Operations.OrchestratedSequenceWork:
                case Operations.OrchestratedSequenceData:
                case Operations.OrchestratedSequenceLongHttp:
                case Operations.OrchestratedSequenceWorkHttp:
                case Operations.OrchestratedSequenceDataHttp:
                case Operations.TriggeredSequenceLong:
                case Operations.TriggeredSequenceWork:
                case Operations.TriggeredSequenceData:
                case Operations.QueueSequenceLong:
                case Operations.QueueSequenceWork:
                case Operations.QueueSequenceData:
                case Operations.AwsTriggeredSequenceLong:
                case Operations.AwsTriggeredSequenceWork:
                case Operations.AwsTriggeredSequenceData:
                    {
                        string opname = operation.ToString();
                        string orchestrationName = 
                            opname.Contains("Triggered") ? "BlobSequence" : 
                            opname.Contains("Queue") ? "QueueSequence" : "OrchestratedSequence";
                        string instanceId = Guid.NewGuid().ToString("N");

                        bool useReportedLatency = !opname.Contains("Http");

                        int defaultLength = 3;
                        int defaultWorkExponent = 0;
                        int defaultDataExponent = 0;

                        object input = new
                        {
                            Length = opname.Contains("Long") ? this.parameters.NumberObjects : defaultLength,
                            WorkExponent = opname.Contains("Work") ? this.parameters.NumberObjects : defaultWorkExponent,
                            DataExponent = opname.Contains("Data") ? this.parameters.NumberObjects : defaultDataExponent,
                        };

                        if (!opname.Contains("Aws"))
                        {
                            logger?.LogTrace($"issued orchestration name={orchestrationName} instanceId={instanceId} input={JsonConvert.SerializeObject(input, Formatting.None)}");

                            var tuple = await this.client.RunOrchestrationAsync(logger, orchestrationName, instanceId, input, useReportedLatency).ConfigureAwait(false);

                            logger?.LogTrace($"orchestration completed id={instanceId} result={tuple.Result}");

                            return tuple;
                        }
                        else
                        {
                            string stateMachineArn = AwsParameters.TriggeredSequence_Arn;
                            JObject jinput = JObject.FromObject(new { input = input });
                            Client.CallbackResponse response = await this.client.RunWrappedStepFunctionAsync(this.callbackUri, stateMachineArn, jinput);
                            return new StatTuple() { Time = response.EndTime, Duration = response.CompletionTime };
                        }
                    }

                case Operations.BankNoConflicts:
                case Operations.BankNoConflictsHttp:
                case Operations.Bank:
                    {
                        int target;
                        if (operation == Operations.BankNoConflicts || operation == Operations.BankNoConflictsHttp)
                        {
                            target = this.privateIndex;
                        }
                        else
                        {
                            // pair of accounts is chosen randomly from available
                            target = this.random.Next(this.parameters.NumberObjects);
                        }

                        string name = "BankTransaction";
                        var instanceId = $"Bank-{Guid.NewGuid():n}-!{target % 32:D2}";

                        bool useReportedLatency = string.IsNullOrEmpty(this.parameters.EventHubsConnection) && operation != Operations.BankNoConflictsHttp;

                        logger?.LogTrace($"issued id={instanceId} target={target}");

                        var tuple = await this.client.RunOrchestrationAsync(logger, name, instanceId, target, useReportedLatency).ConfigureAwait(false);

                        logger?.LogTrace($"received id={instanceId} result={tuple.Result}");

                        return tuple;
                    }

                case Operations.CallbackTest:
                    {
                        string requestUri = this.client.BaseUrl() + "/callbackTest";
                        JObject input(string callbackUri) => new JObject(new JProperty("CallbackUri", callbackUri));               
                        Client.CallbackResponse response = await this.client.RunRemoteRequestWithCallback(this.callbackUri, requestUri, input);
                        return new StatTuple() { Time = response.EndTime, Duration = response.CompletionTime };
                    }

                case Operations.HelloAWS:
                    {
                        string stateMachineArn = AwsParameters.Hello_Arn;
                        //JObject input = JObject.Parse("{ \"array\": [ {}, {}, {} ] }");
                        JObject input = JObject.Parse("{ \"array\": [ {} ] }");
                        Client.CallbackResponse response = await this.client.RunWrappedStepFunctionAsync(this.callbackUri, stateMachineArn, input);
                        return new StatTuple() { Time = response.EndTime, Duration = response.CompletionTime };
                    }

                case Operations.ImageAWS:
                    {
                        string stateMachineArn = AwsParameters.Image_Arn;
                        var input = new JObject(
                            new JProperty("s3Bucket", AwsParameters.Image_s3Bucket),
                            new JProperty("s3Key", AwsParameters.Image_s3Key),
                            new JProperty("objectID", AwsParameters.Image_objectID));
                        Client.CallbackResponse response = await this.client.RunWrappedStepFunctionAsync(this.callbackUri, stateMachineArn, input);
                        return new StatTuple() { Time = response.EndTime, Duration = response.CompletionTime };
                    }

                case Operations.ImageDF:
                    {
                        string name = "ImageRecognitionOrchestration";
                        var instanceId = $"Image-{Guid.NewGuid():n}";
                        var objectInput = new
                        {
                            extractImageMetadataURI = AwsParameters.Image_extractImageMetadataURI,
                            transformMetadataURI = AwsParameters.Image_transformMetadataURI,
                            rekognitionURI = AwsParameters.Image_rekognitionURI,
                            generateThumbnailURI = AwsParameters.Image_generateThumbnailURI,
                            storeImageMetadataURI = AwsParameters.Image_storeImageMetadataURI,
                            s3Prefix = AwsParameters.Image_s3Prefix,
                            s3Bucket = AwsParameters.Image_s3Bucket,
                            s3Key = AwsParameters.Image_s3Key,
                            objectID = AwsParameters.Image_objectID
                        };
                        var input = JsonConvert.SerializeObject(objectInput);

                        logger?.LogTrace($"issued id={instanceId} ");

                        var tuple = await this.client.RunOrchestrationAsync(logger, name, instanceId, input, true).ConfigureAwait(false);

                        logger?.LogTrace($"received id={instanceId} result={tuple.Result}");

                        return tuple;
                    }

                default: throw new InvalidOperationException();
            }
        }
    }
}
