// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
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
        public static bool IsTransientStorageError(StorageException e, CancellationToken token)
        {
            // Transient error codes as documented at https://docs.microsoft.com/en-us/azure/architecture/best-practices/retry-service-specific#azure-storage
            if ( (e.RequestInformation.HttpStatusCode == 408)    //408 Request Timeout
                || (e.RequestInformation.HttpStatusCode == 429)  //429 Too Many Requests
                || (e.RequestInformation.HttpStatusCode == 500)  //500 Internal Server Error
                || (e.RequestInformation.HttpStatusCode == 502)  //502 Bad Gateway
                || (e.RequestInformation.HttpStatusCode == 503)  //503 Service Unavailable
                || (e.RequestInformation.HttpStatusCode == 504)) //504 Gateway Timeout
            {
                return true;
            }

            // Empirically observed transient cancellation exceptions that are not application initiated
            if (e.InnerException is TaskCanceledException && !token.IsCancellationRequested)
            {
                return true;
            }

            // Empirically observed transient exception 
            // ('An existing connection was forcibly closed by the remote host')
            if (e.InnerException is System.Net.Http.HttpRequestException 
                && e.InnerException?.InnerException is System.IO.IOException)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Checks whether the given storage exception is a timeout exception.
        /// </summary>
        /// <param name="e">The storage exception.</param>
        /// <returns>Whether this is a timeout storage exception.</returns>
        public static bool IsTimeout(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 408);  //408 Request Timeout
        }

        // Lease error codes are documented at https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob

        public static bool LeaseConflictOrExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409) || (e.RequestInformation.HttpStatusCode == 412);
        }

        public static bool LeaseConflict(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409);
        }

        public static bool LeaseExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 412);
        }

        public static bool CannotDeleteBlobWithLease(StorageException e)
        {
            return (e.RequestInformation?.HttpStatusCode == 412);
        }

        public static bool BlobDoesNotExist(StorageException e)
        {
            var information = e.RequestInformation.ExtendedErrorInformation;
            return (e.RequestInformation.HttpStatusCode == 404) && (information.ErrorCode.Equals(BlobErrorCodeStrings.BlobNotFound));
        }
    }
}
