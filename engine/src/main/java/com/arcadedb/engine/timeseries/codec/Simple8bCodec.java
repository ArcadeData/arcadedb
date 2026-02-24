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

import java.nio.ByteBuffer;

/**
 * Simple-8b encoding for signed integer arrays using zigzag encoding.
 * Signed values are converted to non-negative via zigzag encoding before packing.
 * <p>
 * <b>Supported value range:</b> [-(2^59), (2^59)-1].
 * Values outside this range cause encode() to throw {@link IllegalArgumentException}
 * because the maximum selector packs 1 value × 60 bits and ZigZag encoding of the
 * boundary value -(2^59) produces exactly (1L&lt;&lt;60)-1, which is the largest encodable value.
 * Values with |v| &gt;= 2^59 would silently truncate — validation prevents silent data corruption.
 * <p>
 * Packs multiple integers into 64-bit words using a selector scheme.
 * The top 4 bits of each word are the selector (0-14), determining how many
 * integers are packed and at what bit width.
 * <p>
 * Selector table (selector → count × bits):
 * 0: 240×0 (all zeros), 1: 120×0 (all zeros, half), 2: 60×1, 3: 30×2,
 * 4: 20×3, 5: 15×4, 6: 12×5, 7: 10×6, 8: 8×7, 9: 7×8, 10: 6×10,
 * 11: 5×12, 12: 4×15, 13: 3×20, 14: 2×30, 15: 1×60
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class Simple8bCodec {

  // selector → number of integers packed
  private static final int[] SELECTOR_COUNT = { 240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1 };
  // selector → bits per integer
  private static final int[] SELECTOR_BITS  = { 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60 };

  // Maximum zigzag-encoded value that fits in 60 bits (selector 15 = 1 value × 60 bits)
  static final long MAX_ZIGZAG_VALUE = (1L << 60) - 1;

  private Simple8bCodec() {
  }

  public static byte[] encode(final long[] values) {
    if (values == null || values.length == 0)
      return new byte[0];

    // Zigzag-encode signed longs to non-negative values before packing.
    // Validate that each zigzag-encoded value fits in 60 bits; values outside
    // [-(2^59), (2^59)-1] cannot be represented and would silently truncate.
    final long[] zigzagged = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      final long encoded = zigzagEncode(values[i]);
      if (encoded > MAX_ZIGZAG_VALUE)
        throw new IllegalArgumentException(
            "Value " + values[i] + " at index " + i + " is outside the Simple-8b supported range [-(2^59), (2^59)-1]");
      zigzagged[i] = encoded;
    }

    // Worst case: each value needs its own word + header
    final ByteBuffer buf = ByteBuffer.allocate(4 + (zigzagged.length + 1) * 8);
    buf.putInt(zigzagged.length);

    int pos = 0;
    while (pos < zigzagged.length) {
      final int remaining = zigzagged.length - pos;

      // Find the best selector
      int bestSelector = 15; // fallback: 1 value × 60 bits
      for (int sel = 0; sel < 16; sel++) {
        final int count = Math.min(SELECTOR_COUNT[sel], remaining);
        final int bits = SELECTOR_BITS[sel];

        if (count <= 0)
          continue;

        boolean fits = true;
        if (bits == 0) {
          // All must be zero
          for (int j = 0; j < count; j++) {
            if (zigzagged[pos + j] != 0) {
              fits = false;
              break;
            }
          }
        } else {
          final long maxVal = (1L << bits) - 1;
          for (int j = 0; j < count; j++) {
            if (zigzagged[pos + j] > maxVal) {
              fits = false;
              break;
            }
          }
        }

        if (fits) {
          bestSelector = sel;
          break; // Take the first (most compact) selector that fits
        }
      }

      // Encode the word
      final int count = Math.min(SELECTOR_COUNT[bestSelector], remaining);
      final int bits = SELECTOR_BITS[bestSelector];
      long word = (long) bestSelector << 60;

      if (bits > 0) {
        for (int j = 0; j < count; j++)
          word |= (zigzagged[pos + j] & ((1L << bits) - 1)) << (j * bits);
      }

      buf.putLong(word);
      pos += count;
    }

    buf.flip();
    final byte[] result = new byte[buf.remaining()];
    buf.get(result);
    return result;
  }

  public static long[] decode(final byte[] data) throws java.io.IOException {
    if (data == null || data.length == 0)
      return new long[0];

    try {
      final ByteBuffer buf = ByteBuffer.wrap(data);
      final int totalCount = buf.getInt();
      if (totalCount < 0)
        throw new java.io.IOException("Simple8bCodec: negative count " + totalCount + " in header");
      final long[] result = new long[totalCount];

      int pos = 0;
      while (pos < totalCount) {
        final long word = buf.getLong();
        final int selector = (int) (word >>> 60) & 0xF;
        final int count = Math.min(SELECTOR_COUNT[selector], totalCount - pos);
        final int bits = SELECTOR_BITS[selector];

        if (bits == 0) {
          // All zeros — result is already initialized to 0
          pos += count;
        } else {
          final long mask = (1L << bits) - 1;
          for (int j = 0; j < count; j++) {
            result[pos + j] = (word >>> (j * bits)) & mask;
          }
          pos += count;
        }
      }
      // Zigzag-decode back to signed values
      for (int i = 0; i < result.length; i++)
        result[i] = zigzagDecode(result[i]);
      return result;
    } catch (final java.nio.BufferUnderflowException e) {
      throw new java.io.IOException("Simple8bCodec: malformed data (truncated buffer, size=" + data.length + ")", e);
    }
  }

  private static long zigzagEncode(final long n) {
    return (n << 1) ^ (n >> 63);
  }

  private static long zigzagDecode(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }
}
