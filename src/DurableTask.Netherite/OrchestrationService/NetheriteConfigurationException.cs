// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// An exception that indicates configuration errors for Netherite.
    /// </summary>
    [Serializable()]
    public class NetheriteConfigurationException : System.Exception
    {
        public NetheriteConfigurationException() : base() { }
        public NetheriteConfigurationException(string message) : base(message) { }
        public NetheriteConfigurationException(string message, System.Exception inner) : base(message, inner) { }

        protected NetheriteConfigurationException(System.Runtime.Serialization.SerializationInfo info,
         System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
