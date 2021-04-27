// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
namespace DurableTask.Netherite.Faster
{
#pragma warning disable 0162
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    /// <summary>
    /// Recovery info for FASTER Log
    /// Needs to stay in sync with FASTER source.
    /// </summary>
    public static class FasterLogRecoveryInfo
    {
        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public static byte[] ModifyCommitPosition(byte[] info, long maxPosition)
        {
            int version;
            long checkSum;
            long beginAddress;
            long flushedUntilAddress;

            using (var ms = new MemoryStream(info))
            {
                using var reader = new BinaryReader(ms);

                try
                {
                    version = reader.ReadInt32();
                    checkSum = reader.ReadInt64();
                    beginAddress = reader.ReadInt64();
                    flushedUntilAddress = reader.ReadInt64();
                }
                catch (Exception e)
                {
                    throw new FormatException("Unable to recover from previous commit. Inner exception: " + e.ToString());
                }
                if (version != 0)
                    throw new FormatException("Invalid version found during commit recovery");

                if (checkSum != (beginAddress ^ flushedUntilAddress))
                    throw new FormatException("Invalid checksum found during commit recovery");

                var count = 0;
                try
                {
                    count = reader.ReadInt32();
                }
                catch { }

                if (count > 0)
                {
                    throw new FormatException("Unexpected iterators");
                }

                flushedUntilAddress = Math.Min(maxPosition, flushedUntilAddress);

                ms.Seek(0, SeekOrigin.Begin);
                using var writer = new BinaryWriter(ms);

                writer.Write(0); // version
                writer.Write(beginAddress ^ flushedUntilAddress); // checksum
                writer.Write(beginAddress);
                writer.Write(flushedUntilAddress);
                writer.Write(0);

                return ms.ToArray();
            }
        }
    }
}