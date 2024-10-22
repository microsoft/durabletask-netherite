// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;

    /// <summary>
    /// We still keep a few utility functions around from the Microsoft.Azure.Storage (V11) SDK. There are no V12 SDK equivalents.
    /// We should consider replacing these with some alternative next time we do a major version bump, and remove the V11 dependency.
    /// </summary>
    static class BlobUtilsV11
    {
        public static void ParseStorageConnectionString(string connectionString, out string accountName, out Uri tableEndpoint, out Uri blobEndpoint, out Uri queueEndpoint)
        {
            var cloudStorageAccount = Microsoft.Azure.Storage.CloudStorageAccount.Parse(connectionString);

            accountName = cloudStorageAccount.Credentials.AccountName;
            tableEndpoint = cloudStorageAccount.TableEndpoint;
            blobEndpoint = cloudStorageAccount.BlobEndpoint;
            queueEndpoint = cloudStorageAccount.QueueEndpoint;
        }

        public static void ValidateContainerName(string name)
        {
            Microsoft.Azure.Storage.NameValidator.ValidateContainerName(name.ToLowerInvariant());
        }

        public static void ValidateBlobName(string name)
        {
            Microsoft.Azure.Storage.NameValidator.ValidateBlobName(name);
        }
    }
}
