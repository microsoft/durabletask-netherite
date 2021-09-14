// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.MemoryBalloon
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public class MemoryBalloonEntity
    {
        // we make this large in memory only, but not storage, by using a custom serializer
        [JsonConverter(typeof(CustomConverter))]
        public List<byte[]> Balloon { get; set; } = new List<byte[]>();

        static ILogger logger;

        public MemoryBalloonEntity(ILogger logger)
        {
            MemoryBalloonEntity.logger = logger;
        }

        public class CustomConverter : JsonConverter<List<byte[]>>
        {
            public override void WriteJson(JsonWriter writer, List<byte[]> value, JsonSerializer serializer)
            {
                MemoryBalloonEntity.logger?.LogWarning("MemoryBalloonEntity: Write {length}", value.Count);
                writer.WriteValue(value.Count.ToString());
            }
   
            public override List<byte[]> ReadJson(JsonReader reader, Type objectType, [AllowNull] List<byte[]> existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                string s = (string)reader.Value;
                var length = int.Parse(s);
                var list = new List<byte[]>(length);
                for (int i = 0; i < length; i++)
                {
                    list.Add(new byte[1024 * 1024]);
                }
                MemoryBalloonEntity.logger?.LogWarning("MemoryBalloonEntity: Read {length}", list.Count);
                return list;
            }
        }

        public void Inflate(int amount)
        {
            for (int i = 0; i < amount; i++)
            {
                // add approx. 1 MB to the memory
                this.Balloon.Add(new byte[1024 * 1024]);
            }
        }

        public void Deflate()
        {
            this.Balloon.Clear();
        }

        [FunctionName(nameof(MemoryBalloonEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx, ILogger logger)
            => ctx.DispatchAsync<MemoryBalloonEntity>(logger);
    }
}
