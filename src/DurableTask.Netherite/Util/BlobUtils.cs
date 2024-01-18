// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Models;
    using DurableTask.Core.Common;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.Blob.Protocol;

    static class BlobUtils
    {
        /// <summary>
        /// Forcefully deletes a blob.
        /// </summary>
        /// <param name="blob">The CloudBlob to delete.</param>
        /// <returns>A task that completes when the operation is finished.</returns>
        public static async Task<bool> ForceDeleteAsync(CloudBlob blob)
        {
            try
            {
                await blob.DeleteAsync();
                return true;
            }
            catch (StorageException e) when (BlobDoesNotExist(e))
            {
                return false;
            }
            catch (StorageException e) when (CannotDeleteBlobWithLease(e))
            {
                try
                {
                    await blob.BreakLeaseAsync(TimeSpan.Zero).ConfigureAwait(false);
                }
                catch
                {
                    // we ignore exceptions in the lease breaking since there could be races
                }

                // retry the delete
                try
                {
                    await blob.DeleteAsync().ConfigureAwait(false);
                    return true;
                }
                catch (StorageException ex) when (BlobDoesNotExist(ex))
                {
                    return false;
                }
            }
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
            // handle Azure V11 SDK exceptions
            if (exception is StorageException e && httpStatusIndicatesTransientError(e.RequestInformation?.HttpStatusCode))
            {
                return true;
            }

            // handle Azure V12 SDK exceptions
            if (exception is Azure.RequestFailedException e1 && httpStatusIndicatesTransientError(e1.Status))
            {
                return true;
            }

            if (exception is ForceRetryException)
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

            return false;
        }

        /// <summary>
        /// Checks whether the given exception is a timeout exception.
        /// </summary>
        /// <param name="e">The exception.</param>
        /// <returns>Whether this is a timeout storage exception.</returns>
        public static bool IsTimeout(Exception exception)
        {
            return exception is System.TimeoutException
                || (exception is StorageException e && e.RequestInformation?.HttpStatusCode == 408)  //408 Request Timeout
                || (exception is Azure.RequestFailedException e1 && (e1.Status == 408 || e1.ErrorCode == "OperationTimedOut"))
                || (exception is TaskCanceledException & exception.Message.StartsWith("The operation was cancelled because it exceeded the configured timeout"));
        }

        // Transient http status codes as documented at https://docs.microsoft.com/en-us/azure/architecture/best-practices/retry-service-specific#azure-storage
        static bool httpStatusIndicatesTransientError(int? statusCode) =>
            (statusCode == 408    //408 Request Timeout
            || statusCode == 429  //429 Too Many Requests
            || statusCode == 500  //500 Internal Server Error
            || statusCode == 502  //502 Bad Gateway
            || statusCode == 503  //503 Service Unavailable
            || statusCode == 504); //504 Gateway Timeout


        /// <summary>
        /// A custom exception class that we use to explicitly force a retry after a transient error.
        /// By using an exception we ensure that we stay under the total retry count and generate the proper tracing.
        /// </summary>
        public class ForceRetryException : Exception
        {
            public ForceRetryException()
            {
            }

            public ForceRetryException(string message)
                : base(message)
            {
            }

            public ForceRetryException(string message, Exception inner)
                : base(message, inner)
            {
            }
        }

        // Lease error codes are documented at https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob

        public static bool LeaseConflictOrExpired(StorageException e)
        {
            return (e.RequestInformation?.HttpStatusCode == 409) || (e.RequestInformation?.HttpStatusCode == 412);
        }

        public static bool LeaseConflict(StorageException e)
        {
            return (e.RequestInformation?.HttpStatusCode == 409);
        }

        public static bool LeaseExpired(StorageException e)
        {
            return (e.RequestInformation?.HttpStatusCode == 412);
        }

        public static bool CannotDeleteBlobWithLease(StorageException e)
        {
            return (e.RequestInformation?.HttpStatusCode == 412);
        }

        public static bool BlobDoesNotExist(StorageException e)
        {
            var information = e.RequestInformation?.ExtendedErrorInformation;
            return (e.RequestInformation?.HttpStatusCode == 404) && (information.ErrorCode.Equals(BlobErrorCodeStrings.BlobNotFound));
        }

        public static bool BlobAlreadyExists(StorageException e)
        {
            return (e.RequestInformation?.HttpStatusCode == 409);
        }
    }
}
