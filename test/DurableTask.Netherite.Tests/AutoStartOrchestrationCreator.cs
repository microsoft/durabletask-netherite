// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Tests
{
    using DurableTask.Core;
    using System;

    /// <summary>
    /// Autostart-Orchestration instance creator for a type, prefixes type name with '@'
    /// </summary>
    /// <typeparam name="T">Type of Orchestration</typeparam>
    public class AutoStartOrchestrationCreator : ObjectCreator<TaskOrchestration>
    {
        readonly TaskOrchestration instance;
        readonly Type prototype;

        /// <summary>
        /// Creates a new AutoStartOrchestrationCreator of supplied type
        /// </summary>
        /// <param name="type">Type to use for the creator</param>
        public AutoStartOrchestrationCreator(Type type)
        {
            this.prototype = type;
            this.Initialize(type);
        }

        /// <summary>
        /// Creates a new AutoStartOrchestrationCreator using type of supplied object instance
        /// </summary>
        /// <param name="instance">Object instances to infer the type from</param>
        public AutoStartOrchestrationCreator(TaskOrchestration instance)
        {
            this.instance = instance;
            this.Initialize(instance);
        }

        ///<inheritdoc/>
        public override TaskOrchestration Create()
        {
            if (this.prototype != null)
            {
                return (TaskOrchestration)Activator.CreateInstance(this.prototype);
            }

            return this.instance;
        }

        void Initialize(object obj)
        {
            this.Name = $"@{NameVersionHelper.GetDefaultName(obj)}";
            this.Version = NameVersionHelper.GetDefaultVersion(obj);
        }
    }
}