// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class Results
    {
        public BaseParameters parameters { get; set; }

        public Coordinates coordinates { get; set; }

        public JToken results { get; set; }


        public class Coordinates
        {
            public string testname { get; set; }

            public string scenarioname { get; set; }

            public string id { get; set; }
        }

        [FunctionName(nameof(SaveResults))]
        public static void SaveResults([ActivityTrigger] Results results)
        {
            var testname = results.coordinates.testname;
            var scenarioid = results.coordinates.id;
            var resultfiledirectory = GetOrCreateResultFileDirectory(testname).Result;
            var blobname = string.Format("result-{0}.json", scenarioid);
            var resultblob = resultfiledirectory.GetBlockBlobReference(blobname);
            resultblob.Properties.ContentType = "application/json";
            resultblob.UploadText(JsonConvert.SerializeObject(results, Formatting.Indented));
        }

        static readonly string RESULTS_CONNECTION = "ResultsConnection";
        static readonly string RESULTS_CONNECTION_DEFAULT = "AzureWebJobsStorage";
        static readonly string RESULTFILE_CONTAINER = "results";

        public static string MakeTestName(string benchmarkname)
        {
            return string.Format("{1}-{0:o}", DateTime.UtcNow, benchmarkname);
        }

        public static async Task<CloudBlobDirectory> GetOrCreateResultFileDirectory(string testname)
        {
            string connectionString = Environment.GetEnvironmentVariable(RESULTS_CONNECTION);
            if (string.IsNullOrEmpty(connectionString))
            {
                connectionString = Environment.GetEnvironmentVariable(RESULTS_CONNECTION_DEFAULT);
            }
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobclient = account.CreateCloudBlobClient();
            CloudBlobContainer container = blobclient.GetContainerReference(RESULTFILE_CONTAINER);
            await container.CreateIfNotExistsAsync();
            CloudBlobDirectory dir = container.GetDirectoryReference(testname);
            return dir;
        }
    }
}