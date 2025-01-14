// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Net.Http;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;
    using Newtonsoft.Json;
    using static DurableTask.Netherite.TransportAbstraction;

    static class BlobUtilsV12
    {
        static readonly BlobUploadOptions JsonBlobUploadOptions = new() { HttpHeaders = new BlobHttpHeaders() { ContentType = "application/json" } };

        public class ServerTimeoutPolicy : HttpPipelineSynchronousPolicy
        {
            readonly int timeout;

            public ServerTimeoutPolicy(int timeout)
            {
                this.timeout = timeout;
            }

            public override void OnSendingRequest(HttpMessage message)
            {
                message.Request.Uri.AppendQuery("timeout", this.timeout.ToString());
            }
        }
        public struct ServiceClients
        {
            public BlobServiceClient Default;
            public BlobServiceClient Aggressive;
            public BlobServiceClient WithRetries;
        }

        internal static ServiceClients GetServiceClients(ConnectionInfo info)
        {
            var aggressiveOptions = new BlobClientOptions();
            aggressiveOptions.Retry.MaxRetries = 0;
            aggressiveOptions.Retry.NetworkTimeout = TimeSpan.FromSeconds(3);
            aggressiveOptions.AddPolicy(new ServerTimeoutPolicy(2), HttpPipelinePosition.PerCall);

            var defaultOptions = new BlobClientOptions();
            defaultOptions.Retry.MaxRetries = 0;
            defaultOptions.Retry.NetworkTimeout = TimeSpan.FromSeconds(16);
            defaultOptions.AddPolicy(new ServerTimeoutPolicy(15), HttpPipelinePosition.PerCall);

            var withRetriesOptions = new BlobClientOptions();
            withRetriesOptions.Retry.MaxRetries = 10;
            withRetriesOptions.Retry.Mode = RetryMode.Exponential;
            withRetriesOptions.Retry.Delay = TimeSpan.FromSeconds(1);
            withRetriesOptions.Retry.MaxDelay = TimeSpan.FromSeconds(30);
           
            return new ServiceClients()
            {
                Default = info.GetAzureStorageV12BlobServiceClient(defaultOptions),
                Aggressive = info.GetAzureStorageV12BlobServiceClient(aggressiveOptions),
                WithRetries = info.GetAzureStorageV12BlobServiceClient(withRetriesOptions),
            };
        }

        public struct ContainerClients
        {
            public BlobContainerClient Default;
            public BlobContainerClient Aggressive;
            public BlobContainerClient WithRetries;
        }

        internal static ContainerClients GetContainerClients(ServiceClients serviceClients, string blobContainerName)
        {
            return new ContainerClients()
            {
                Default = serviceClients.Default.GetBlobContainerClient(blobContainerName),
                Aggressive = serviceClients.Aggressive.GetBlobContainerClient(blobContainerName),
                WithRetries = serviceClients.WithRetries.GetBlobContainerClient(blobContainerName),
            };

        }

        public struct BlockBlobClients
        {
            public BlockBlobClient Default;
            public BlockBlobClient Aggressive;
            public BlockBlobClient WithRetries;

            public string Name => this.Default?.Name;
        }

        internal static BlockBlobClients GetBlockBlobClients(ContainerClients containerClients, string blobName)
        {
            return new BlockBlobClients()
            {
                Default = containerClients.Default.GetBlockBlobClient(blobName),
                Aggressive = containerClients.Aggressive.GetBlockBlobClient(blobName),
                WithRetries = containerClients.WithRetries.GetBlockBlobClient(blobName),
            };

        }

        public struct PageBlobClients
        {
            public PageBlobClient Default;
            public PageBlobClient Aggressive;
        }

        internal static PageBlobClients GetPageBlobClients(ContainerClients containerClients, string blobName)
        {
            return new PageBlobClients()
            {
                Default = containerClients.Default.GetPageBlobClient(blobName),
                Aggressive = containerClients.Aggressive.GetPageBlobClient(blobName),
            };

        }

        public static async Task<T> ReadJsonBlobAsync<T>(BlockBlobClient blobClient, bool throwIfNotFound, bool throwOnParseError, CancellationToken token) where T : class
        {
            try
            {
                var downloadResult = await blobClient.DownloadContentAsync();
                string blobContents = downloadResult.Value.Content.ToString();
                return JsonConvert.DeserializeObject<T>(blobContents);
            }
            catch (RequestFailedException ex)
                when (BlobUtilsV12.BlobDoesNotExist(ex) && !throwIfNotFound)
            {
                // container or blob does not exist
            }
            catch (JsonException) when (!throwOnParseError)
            {
                // cannot parse content of blob
            }

            return null;
        }

        public struct BlobDirectory
        {
            readonly ContainerClients client;
            readonly string prefix;

            public ContainerClients Client => this.client;
            public string Prefix => this.prefix;

            public BlobDirectory(ContainerClients client, string prefix)
            {
                this.client = client;
                this.prefix = string.Concat(prefix);
            }

            public BlobDirectory GetSubDirectory(string path)
            {
                return new BlobDirectory(this.client, $"{this.prefix}/{path}");
            }

            public BlobUtilsV12.BlockBlobClients GetBlockBlobClient(string name)
            {
                return BlobUtilsV12.GetBlockBlobClients(this.client, $"{this.prefix}/{name}");
            }

            public BlobUtilsV12.PageBlobClients GetPageBlobClient(string name)
            {
                return BlobUtilsV12.GetPageBlobClients(this.client, $"{this.prefix}/{name}");
            }

            public async Task<List<string>> GetBlobsAsync(CancellationToken cancellationToken)
            {
                var list = new List<string>();
                await foreach (var blob in this.client.WithRetries.GetBlobsAsync(prefix: this.prefix, cancellationToken: cancellationToken))
                {
                    list.Add(blob.Name);
                }
                return list;
            }

            public override string ToString()
            {
                return $"{this.prefix}/";
            }
        }

        /// <summary>
        /// Forcefully deletes a blob.
        /// </summary>
        /// <param name="blob">The CloudBlob to delete.</param>
        /// <returns>A task that completes when the operation is finished.</returns>
        public static async Task<bool> ForceDeleteAsync(BlobContainerClient containerClient, string blobName)
        {
            var blob = containerClient.GetBlobClient(blobName);

            try
            {
                await blob.DeleteAsync();
                return true;
            }
            catch (Azure.RequestFailedException e) when (BlobDoesNotExist(e))
            {
                return false;
            }
            catch (Azure.RequestFailedException e) when (CannotDeleteBlobWithLease(e))
            {
                try
                {
                    var leaseClient = new BlobLeaseClient(blob);
                    await leaseClient.BreakAsync(TimeSpan.Zero);
                }
                catch
                {
                    // we ignore exceptions in the lease breaking since there could be races
                }

                // retry the delete
                try
                {
                    await blob.DeleteAsync();
                    return true;
                }
                catch (Azure.RequestFailedException ex) when (BlobDoesNotExist(ex))
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Checks whether the given exception is a timeout exception.
        /// </summary>
        /// <param name="e">The exception.</param>
        /// <returns>Whether this is a timeout storage exception.</returns>
        public static bool IsTimeout(Exception exception)
        {
            return exception is System.TimeoutException
                || (exception is Azure.RequestFailedException e1 && (e1.Status == 408 || e1.ErrorCode == "OperationTimedOut"))
                || (exception is TaskCanceledException & exception.Message.StartsWith("The operation was cancelled because it exceeded the configured timeout"));
        }

        /// <summary>
        /// Checks whether the given storage exception is transient, and 
        /// therefore meaningful to retry.
        /// </summary>
        /// <param name="e">The storage exception.</param>
        /// <param name="token">The cancellation token that was passed to the storage request.</param>
        /// <returns>Whether this is a transient storage exception.</returns>
        public static bool IsTransientStorageError(Exception exception)
        { 
            if (exception is Azure.RequestFailedException e1 && httpStatusIndicatesTransientError(e1.Status))
            {
                return true;
            }

            // Empirically observed: timeouts on synchronous calls
            if (exception.InnerException is TimeoutException)
            {
                return true;
            }

            // Empirically observed: transient cancellation exceptions that are not application initiated
            if (exception is OperationCanceledException || exception.InnerException is OperationCanceledException)
            {
                return true;
            }

            // Empirically observed: transient exception ('An existing connection was forcibly closed by the remote host')
            if (exception.InnerException is System.Net.Http.HttpRequestException && exception.InnerException?.InnerException is System.IO.IOException)
            {
                return true;
            }

            // Empirically observed: transient socket exceptions
            if (exception is System.IO.IOException && exception.InnerException is System.Net.Sockets.SocketException)
            {
                return true;
            }

            // Empirically observed: transient DNS failures
            if (exception is Azure.RequestFailedException && exception.InnerException is System.Net.Http.HttpRequestException e2 && e2.Message.Contains("No such host is known"))
            {
                return true;
            }

            // Empirically observed: socket exceptions under heavy stress, such as
            // - "Only one usage of each socket address (protocol/network address/port) is normally permitted"
            // - "An operation on a socket could not be performed because the system lacked sufficient buffer space or because a queue was full"
            if (exception is Azure.RequestFailedException
                && (exception.InnerException is System.Net.Http.HttpRequestException e3)
                && e3.InnerException is System.Net.Sockets.SocketException)
            {
                return true;
            }

            return false;
        }

        static bool httpStatusIndicatesTransientError(int? statusCode) =>
           (statusCode == 408    //408 Request Timeout
           || statusCode == 429  //429 Too Many Requests
           || statusCode == 500  //500 Internal Server Error
           || statusCode == 502  //502 Bad Gateway
           || statusCode == 503  //503 Service Unavailable
           || statusCode == 504); //504 Gateway Timeout


        // Lease error codes are documented at https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob

        public static bool LeaseConflictOrExpired(Azure.RequestFailedException e)
        {
            return e.Status == 409 || e.Status == 412;
        }

        public static bool LeaseConflict(Azure.RequestFailedException e)
        {
            return e.Status == 409;
        }

        public static bool LeaseExpired(Azure.RequestFailedException e)
        {
            return e.Status == 412;
        }

        public static bool CannotDeleteBlobWithLease(Azure.RequestFailedException e)
        {
            return e.Status == 412;
        }

        public static bool PreconditionFailed(Azure.RequestFailedException e)
        {
            return e.Status == 409 || e.Status == 412;
        }

        public static bool BlobDoesNotExist(Azure.RequestFailedException e)
        {
            return e.Status == 404 && e.ErrorCode == BlobErrorCode.BlobNotFound;
        }

        public static bool BlobOrContainerDoesNotExist(Azure.RequestFailedException e)
        {
            return e.Status == 404 && (e.ErrorCode == BlobErrorCode.BlobNotFound || e.ErrorCode == BlobErrorCode.ContainerNotFound);
        }

        public static bool BlobAlreadyExists(Azure.RequestFailedException e)
        {
            return e.Status == 409;
        }

        public static bool IsCancelled(Azure.RequestFailedException e)
        {
            return e.InnerException != null && e.InnerException is OperationCanceledException;
        }
    }
}
