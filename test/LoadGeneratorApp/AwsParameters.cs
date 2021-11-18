// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public static class AwsParameters
    {
        // The URL at which to call AWS
        public const string ServiceUrl = "...";

        // The ARN for the storage-triggered sequence
        public const string TriggeredSequence_Arn = "...";

        // The ARN of the Hello3 step function sequence
        public const string Hello_Arn = "...";

        // The ARN of the wrapper which runs a step function and returns the start and end time
        public const string SyncWrapper_Arn = "...";

        // The ARN and the parameters for the image-processing step-function workflow
        public const string Image_Arn = "...";
        public const string Image_extractImageMetadataURI = "...";
        public const string Image_transformMetadataURI = "...";
        public const string Image_rekognitionURI = "...";
        public const string Image_generateThumbnailURI = "...";
        public const string Image_storeImageMetadataURI = "...";
        public const string Image_s3Prefix = "...";
        public const string Image_s3Bucket = "...";
        public const string Image_s3Key = "...";
        public const string Image_objectID = "...";
    }
}
