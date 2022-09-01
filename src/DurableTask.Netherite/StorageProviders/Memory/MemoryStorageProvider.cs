// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Faster;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;

    class MemoryStorageProvider : IStorageProvider
    {
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly ILogger logger;

        TaskhubParameters taskhub;

        public MemoryStorageProvider(NetheriteOrchestrationServiceSettings settings, ILogger logger)
        {
            this.settings = settings;
            this.logger = logger;
        }

        void Reset()
        {
            this.taskhub = null;
        }

        public CancellationToken Termination => CancellationToken.None;

        ILoadPublisherService IStorageProvider.LoadPublisher => null; // we do not use this for memory emulation

        async Task<bool> IStorageProvider.CreateTaskhubIfNotExistsAsync()
        {
            await Task.Yield();
            if (this.taskhub == null)
            {
                this.taskhub = new TaskhubParameters()
                {
                    TaskhubName = this.settings.HubName,
                    TaskhubGuid = Guid.NewGuid(),
                    CreationTimestamp = DateTime.UtcNow,
                    StorageFormat = String.Empty,
                    PartitionCount = 1,
                };
                return true;
            }
            else
            {
                return false;
            }
        }

        async Task IStorageProvider.DeleteTaskhubAsync()
        {
            await Task.Yield();
            this.taskhub = null;
        }

        IPartitionState IStorageProvider.CreatePartitionState(TaskhubParameters parameters)
        {
            return new MemoryStorage(this.logger);
        }

        (string containerName, string path) IStorageProvider.GetTaskhubPathPrefix(TaskhubParameters parameters)
        {
            return (string.Empty, string.Empty);
        }

        async Task<TaskhubParameters> IStorageProvider.TryLoadTaskhubAsync(bool throwIfNotFound)
        {
            await Task.Yield();
            return this.taskhub;
        }
    }
}