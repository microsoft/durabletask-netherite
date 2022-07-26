// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json.Linq;
    using System.Net.Http;
    using System.Linq;
    using System.Threading;

    /// <summary>
    /// A workflow for image recognition, adapted from the step-function 
    /// implementation at https://github.com/aws-samples/lambda-refarch-imagerecognition
    /// </summary>
    public static class ImageRecognitionCaseStudy
    {
        /// <summary>
        /// It is an orchestration that executes the image Recognition orchestration
        /// </summary>
        [FunctionName(nameof(ImageRecognitionOrchestration))]
        public static async Task<string> ImageRecognitionOrchestration([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var inputJsonString = context.GetInput<string>();
            dynamic inputJson = JToken.Parse(inputJsonString);
            string s3Bucket = inputJson.s3Bucket;
            string s3Key = inputJson.s3Key;
            string objectID = inputJson.objectID;

            // Execute ExtractImageMetadata
            string extractedMetadataJsonString = await MaskopyCaseStudy.MakeHttpRequestSync("ExtractImageMetadata", (string)inputJson.extractImageMetadataURI, inputJsonString, context, log);
            dynamic extractedMetadataJson = JToken.Parse(extractedMetadataJsonString);
            inputJson.extractedMetadata = extractedMetadataJson;

            // Execute Image Type Check
            string format = extractedMetadataJson.format;
            if (!(format == "JPEG" ||
                  format == "PNG"))
            {
                throw new Exception($"image type {format} not supported");
            }

            // Transform Metadata
            string cleanMetadataJsonString = await MaskopyCaseStudy.MakeHttpRequestSync("TransformMetadata", (string)inputJson.transformMetadataURI, extractedMetadataJsonString, context, log);
            dynamic cleanMetadataJson = JToken.Parse(cleanMetadataJsonString);
            inputJson.extractedMetadata = cleanMetadataJson;

            // Parallel Image recognition and thumbnail
            string rekognitionInput = JsonConvert.SerializeObject(inputJson);
            var rekognitionTask = MaskopyCaseStudy.MakeHttpRequest("Rekognition", (string)inputJson.rekognitionURI, rekognitionInput, context, log);
            var generateThumbnailTask = MaskopyCaseStudy.MakeHttpRequest("GenerateThumbnail", (string)inputJson.generateThumbnailURI, rekognitionInput, context, log);

            // Gather both the results
            // (Maybe) TODO: Make this more efficient using whenAny
            var maxAttempts = 2;
            var intervalSeconds = 1;
            var backoffRate = 1.5;
            var currentRetry = 0;

            var rekognitionResult = await rekognitionTask;
            while (rekognitionResult.StatusCode != System.Net.HttpStatusCode.OK && currentRetry < maxAttempts)
            {
                DateTime retryWait = context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(intervalSeconds));
                await context.CreateTimer(retryWait, CancellationToken.None);
                if (!context.IsReplaying) log.LogWarning("Retrying calling Rekognition!");
                rekognitionTask = MaskopyCaseStudy.MakeHttpRequest("Rekognition", (string)inputJson.rekognitionURI, rekognitionInput, context, log);
                //rekognitionTask = context.CallHttpAsync(System.Net.Http.HttpMethod.Post, uri, rekognitionInput);
                currentRetry++;
                intervalSeconds = Convert.ToInt32(intervalSeconds * backoffRate);
                rekognitionResult = await rekognitionTask;
            }
            string tagsJsonString = MaskopyCaseStudy.GetHttpResult("Rekognition", rekognitionResult, context, log);

            var generateThumbnailResult = await generateThumbnailTask;
            string thumbnailJsonString = MaskopyCaseStudy.GetHttpResult("GenerateThumbnail", generateThumbnailResult, context, log);

            // This is a bit hacky
            string parallelResultsEntryString = $"[{tagsJsonString}, {thumbnailJsonString}]";
            dynamic parallelResultsEntry = JToken.Parse(parallelResultsEntryString);
            inputJson.parallelResults = parallelResultsEntry;

            // Execute the StoreImageMetadata
            string storeResultJsonString = await MaskopyCaseStudy.MakeHttpRequestSync("StoreImageMetadata", (string)inputJson.storeImageMetadataURI, JsonConvert.SerializeObject(inputJson), context, log);

            return $"executed orchestration for id: {objectID}. Result: {storeResultJsonString}";
        }
    }
}
