// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System.Text;

    /// <summary>
    /// Fast, non-cryptographic hash function helper.
    /// </summary>
    /// <remarks>
    /// See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function.
    /// Tested with production data and random guids. The result was good distribution.
    /// </remarks>
    static class Fnv1aHashHelper
    {
        const uint FnvPrime = unchecked(16777619);
        const uint FnvOffsetBasis = unchecked(2166136261);

        public static uint ComputeHash(string value)
        {
            return ComputeHash(value, encoding: null);
        }

        public static uint ComputeHash(string value, Encoding encoding)
        {
            return ComputeHash(value, encoding, hash: FnvOffsetBasis);
        }

        public static uint ComputeHash(string value, Encoding encoding, uint hash)
        {
            byte[] bytes = (encoding ?? Encoding.UTF8).GetBytes(value);
            return ComputeHash(bytes, hash);
        }

        public static uint ComputeHash(byte[] array)
        {
            return ComputeHash(array, hash: FnvOffsetBasis);
        }

        public static uint ComputeHash(byte[] array, uint hash)
        {
            for (var i = 0; i < array.Length; i++)
            {
                unchecked
                {
                    hash ^= array[i];
                    hash *= FnvPrime;
                }
            }

            return hash;
        }
    }
}
