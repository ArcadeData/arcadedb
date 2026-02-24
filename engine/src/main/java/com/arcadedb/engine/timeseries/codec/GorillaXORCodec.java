/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.engine.timeseries.codec;

/**
 * Gorilla XOR encoding for floating-point values.
 * XOR consecutive IEEE 754 doubles; store only meaningful bits
 * (leading zeros + trailing zeros + middle block).
 * <p>
 * Encoding scheme for XOR'd value:
 * - xor == 0: store '0' (1 bit) — same as previous
 * - leading/trailing same as previous: store '10' + meaningful bits
 * - otherwise: store '11' + 6-bit leading zeros + 6-bit block length + block bits
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GorillaXORCodec {

  private GorillaXORCodec() {
  }

  public static byte[] encode(final double[] values) {
    if (values == null || values.length == 0)
      return new byte[0];

    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(values.length * 2 + 16);

    // Write count
    writer.writeBits(values.length, 32);
    // Write first value raw
    writer.writeBits(Double.doubleToRawLongBits(values[0]), 64);

    if (values.length == 1)
      return writer.toByteArray();

    int prevLeading = Integer.MAX_VALUE;
    int prevTrailing = 0;
    long prevBits = Double.doubleToRawLongBits(values[0]);

    for (int i = 1; i < values.length; i++) {
      final long currentBits = Double.doubleToRawLongBits(values[i]);
      final long xor = currentBits ^ prevBits;

      if (xor == 0) {
        writer.writeBit(0);
      } else {
        writer.writeBit(1);

        final int leading = Long.numberOfLeadingZeros(xor);
        final int trailing = Long.numberOfTrailingZeros(xor);

        if (leading >= prevLeading && trailing >= prevTrailing) {
          // Case '10': reuse previous block position
          writer.writeBit(0);
          final int blockSize = 64 - prevLeading - prevTrailing;
          writer.writeBits(xor >>> prevTrailing, blockSize);
        } else {
          // Case '11': new block position
          writer.writeBit(1);
          // Cap leading zeros at 63 (6 bits)
          final int cappedLeading = Math.min(leading, 63);
          writer.writeBits(cappedLeading, 6);
          final int blockSize = 64 - cappedLeading - trailing;
          // blockSize ranges 1..64; store (blockSize - 1) to fit in 6 bits
          writer.writeBits(blockSize - 1, 6);
          writer.writeBits(xor >>> trailing, blockSize);

          prevLeading = cappedLeading;
          prevTrailing = trailing;
        }
      }
      prevBits = currentBits;
    }
    return writer.toByteArray();
  }

  public static double[] decode(final byte[] data) {
    if (data == null || data.length == 0)
      return new double[0];

    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);

    final int count = (int) reader.readBits(32);
    if (count <= 0 || count > DeltaOfDeltaCodec.MAX_BLOCK_SIZE)
      throw new IllegalArgumentException("GorillaXOR decode: invalid count " + count + " (expected 1.." + DeltaOfDeltaCodec.MAX_BLOCK_SIZE + ")");
    final double[] result = new double[count];

    long prevBits = reader.readBits(64);
    result[0] = Double.longBitsToDouble(prevBits);

    if (count == 1)
      return result;

    // Initialize to MAX_VALUE to match the encoder's initial state: the first XOR'd value
    // always encodes with a new block position (case '11'), which writes prevLeading/prevTrailing.
    int prevLeading = Integer.MAX_VALUE;
    int prevTrailing = 0;

    for (int i = 1; i < count; i++) {
      if (reader.readBit() == 0) {
        // Same as previous
        result[i] = Double.longBitsToDouble(prevBits);
      } else {
        long xor;
        if (reader.readBit() == 0) {
          // Case '10': reuse previous block position
          final int blockSize = 64 - prevLeading - prevTrailing;
          xor = reader.readBits(blockSize) << prevTrailing;
        } else {
          // Case '11': new block position
          prevLeading = (int) reader.readBits(6);
          final int blockSize = (int) reader.readBits(6) + 1;
          prevTrailing = 64 - prevLeading - blockSize;
          xor = reader.readBits(blockSize) << prevTrailing;
        }
        prevBits = prevBits ^ xor;
        result[i] = Double.longBitsToDouble(prevBits);
      }
    }
    return result;
  }

  /**
   * Decodes into a pre-allocated output buffer, returning the number of decoded values.
   * The output array must be at least as large as the encoded count.
   */
  public static int decode(final byte[] data, final double[] output) {
    if (data == null || data.length == 0)
      return 0;

    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);
    final int count = (int) reader.readBits(32);
    if (count <= 0 || count > DeltaOfDeltaCodec.MAX_BLOCK_SIZE)
      throw new IllegalArgumentException("GorillaXOR decode: invalid count " + count + " (expected 1.." + DeltaOfDeltaCodec.MAX_BLOCK_SIZE + ")");

    long prevBits = reader.readBits(64);
    output[0] = Double.longBitsToDouble(prevBits);

    if (count == 1)
      return count;

    // Initialize to MAX_VALUE to match the encoder's initial state: the first XOR'd value
    // always encodes with a new block position (case '11'), which writes prevLeading/prevTrailing.
    int prevLeading = Integer.MAX_VALUE;
    int prevTrailing = 0;

    for (int i = 1; i < count; i++) {
      if (reader.readBit() == 0) {
        output[i] = Double.longBitsToDouble(prevBits);
      } else {
        long xor;
        if (reader.readBit() == 0) {
          final int blockSize = 64 - prevLeading - prevTrailing;
          xor = reader.readBits(blockSize) << prevTrailing;
        } else {
          prevLeading = (int) reader.readBits(6);
          final int blockSize = (int) reader.readBits(6) + 1;
          prevTrailing = 64 - prevLeading - blockSize;
          xor = reader.readBits(blockSize) << prevTrailing;
        }
        prevBits = prevBits ^ xor;
        output[i] = Double.longBitsToDouble(prevBits);
      }
    }
    return count;
  }
}
