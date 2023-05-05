// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if !NETSTANDARD
#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Host.Scale;

    class NetheriteTargetScaler : ITargetScaler
    {
        readonly NetheriteMetricsProvider metricsProvider;
        readonly DurabilityProvider durabilityProvider;
        readonly TargetScalerResult scaleResult;

        public NetheriteTargetScaler(
            string functionId,
            NetheriteMetricsProvider metricsProvider,
            DurabilityProvider durabilityProvider)
        {
            this.metricsProvider = metricsProvider;
            this.durabilityProvider = durabilityProvider;
            this.scaleResult = new TargetScalerResult();
            this.TargetScalerDescriptor = new TargetScalerDescriptor(functionId);
        }

        public TargetScalerDescriptor TargetScalerDescriptor { get; private set; }

        public Task<TargetScalerResult> GetScaleResultAsync(TargetScalerContext context)
        {
            // Refer to GetTargetRecommendation() in ScaleMonitor
            throw new NotImplementedException();
        }
    }
}
#endif
#endif