#if !NETCOREAPP2_2
namespace DurableTask.Netherite.AzureFunctions
{
    using Microsoft.Azure.WebJobs.Host.Scale;

    /// <summary>
    /// Contains metrics used in scaling for Durable Triggers using a Netherite backend.
    /// </summary>
    public class NetheriteScaleMetrics : ScaleMetrics
    {
        public byte[] Metrics { get; set; }
    }
}
#endif