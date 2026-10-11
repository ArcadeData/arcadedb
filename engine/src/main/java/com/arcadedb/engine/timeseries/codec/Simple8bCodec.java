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

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Simple-8b encoding for signed integer arrays using zigzag encoding.
 * Signed values are converted to non-negative via zigzag encoding before packing.
 * <p>
 * <b>Packed range:</b> [-(2^59), (2^59)-1]. The maximum selector packs 1 value × 60 bits and ZigZag encoding of the
 * boundary value -(2^59) produces exactly (1L&lt;&lt;60)-1, which is the largest packable value. A block holding any
 * value outside this range (a snowflake id, a 64-bit hash, an epoch-nanosecond instant) is written in a raw escape
 * layout instead of being refused (issue #9309): the header count is stored negated, followed by the values as plain
 * 64-bit longs. A non-negative header count is the packed layout, so blocks written before the escape existed decode
 * unchanged. The trade-off: one outlier makes its whole block 8 bytes per value, so a column of mostly small values
 * with an occasional wide one compresses worse for the blocks that hold it.
 * <p>
 * <b>Absent values.</b> A block in which some samples carry no value (a {@code null} measurement, issue #9721) is written in a
 * nullable layout: the header is {@link #NULLABLE_MARKER}, followed by the value count, a bitmap of
 * {@code ceil(count / 64)} longs in which bit {@code i} set means sample {@code i} is absent, and then the values in either of
 * the layouts above with every absent one written as zero. A block without an absent sample keeps the layouts above, byte for
 * byte. A decoder that predates the marker reads it as a negative count it cannot honour and refuses the block, rather than
 * reading the absent samples as zeros.
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

  /**
   * The header of the nullable layout (issue #9721). {@link Integer#MIN_VALUE} is the one count neither of the other layouts can
   * write - the raw layout negates a count of at most {@link Integer#MAX_VALUE} - and the one a decoder that predates it refuses.
   */
  static final int NULLABLE_MARKER = Integer.MIN_VALUE;

  private Simple8bCodec() {
  }

  public static byte[] encode(final long[] values) {
    if (values == null || values.length == 0)
      return new byte[0];

    // Zigzag-encode signed longs to non-negative values before packing. A value whose zigzag form needs more than
    // 60 bits cannot be packed: the whole block falls back to the raw layout
    final long[] zigzagged = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      final long encoded = zigzagEncode(values[i]);
      if (Long.compareUnsigned(encoded, MAX_ZIGZAG_VALUE) > 0)
        return encodeRaw(values);
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

  /**
   * Encodes {@code values} together with the samples that carry no value (issue #9721).
   *
   * @param values the values, with every absent sample already written as zero so it costs no width
   * @param nulls  a bitmap over the samples, bit {@code i} of word {@code i >>> 6} set when sample {@code i} is absent, or
   *               {@code null} when every sample has a value. A bitmap with no bit set is the same as {@code null}
   */
  public static byte[] encode(final long[] values, final long[] nulls) {
    if (nulls == null || values == null || values.length == 0 || !anyBitSet(nulls, values.length))
      return encode(values);

    final byte[] payload = encode(values);
    final int words = bitmapWords(values.length);
    final ByteBuffer buf = ByteBuffer.allocate(4 + 4 + words * 8 + payload.length);
    buf.putInt(NULLABLE_MARKER);
    buf.putInt(values.length);
    // bits past the last sample are dropped, so equal blocks always encode to equal bytes
    final int tail = values.length & 63;
    for (int w = 0; w < words; w++) {
      final long word = w < nulls.length ? nulls[w] : 0L;
      buf.putLong(w == words - 1 && tail != 0 ? word & ((1L << tail) - 1) : word);
    }
    buf.put(payload);
    return buf.array();
  }

  /**
   * The bitmap of the samples of {@code data} that carry no value, in the shape {@link #encode(long[], long[])} takes it, or
   * {@code null} when every sample has one - which is every block written before issue #9721.
   */
  public static long[] nulls(final byte[] data) throws IOException {
    if (data == null || data.length < 4)
      return null;
    try {
      final ByteBuffer buf = ByteBuffer.wrap(data);
      if (buf.getInt() != NULLABLE_MARKER)
        return null;
      final int count = buf.getInt();
      if (count < 0)
        throw new IOException("Simple8bCodec: invalid count " + count + " in the nullable header");
      // Checked before allocating, as the raw layout's count is: a corrupt header must not request a bitmap the buffer cannot fill
      final int words = bitmapWords(count);
      if ((long) words * Long.BYTES > buf.remaining())
        throw new IOException("Simple8bCodec: malformed data (null bitmap of " + words + " word(s) exceeds buffer, size=" + data.length + ")");
      final long[] nulls = new long[words];
      for (int w = 0; w < nulls.length; w++)
        nulls[w] = buf.getLong();
      return nulls;
    } catch (final BufferUnderflowException e) {
      throw new IOException("Simple8bCodec: malformed data (truncated null bitmap, size=" + data.length + ")", e);
    }
  }

  /** Whether sample {@code index} is absent in a bitmap {@link #nulls(byte[])} returned; {@code null} has none. */
  public static boolean isNull(final long[] nulls, final int index) {
    return nulls != null && (nulls[index >>> 6] & (1L << index)) != 0;
  }

  /** The number of 64-bit words a bitmap over {@code count} samples takes. */
  public static int bitmapWords(final int count) {
    // in long arithmetic, so a count near Integer.MAX_VALUE does not wrap
    return (int) (((long) count + 63) >>> 6);
  }

  /** Whether any of the first {@code count} bits is set: a bit past the last sample stands for no sample, so it is ignored. */
  private static boolean anyBitSet(final long[] bits, final int count) {
    final int words = bitmapWords(count);
    final int tail = count & 63;
    for (int w = 0; w < words && w < bits.length; w++) {
      final long word = w == words - 1 && tail != 0 ? bits[w] & ((1L << tail) - 1) : bits[w];
      if (word != 0)
        return true;
    }
    return false;
  }

  private static byte[] encodeRaw(final long[] values) {
    final ByteBuffer buf = ByteBuffer.allocate(4 + values.length * 8);
    buf.putInt(-values.length);
    for (final long value : values)
      buf.putLong(value);
    return buf.array();
  }

  public static long[] decode(final byte[] data) throws IOException {
    if (data == null || data.length == 0)
      return new long[0];

    try {
      ByteBuffer buf = ByteBuffer.wrap(data);
      int totalCount = buf.getInt();
      if (totalCount == NULLABLE_MARKER) {
        // The nullable layout (issue #9721): the values follow the bitmap in one of the two layouts below, absent ones as zero
        final int count = buf.getInt();
        if (count < 0)
          throw new IOException("Simple8bCodec: invalid count " + count + " in the nullable header");
        final long payloadStart = 4L + 4L + (long) bitmapWords(count) * Long.BYTES;
        if (payloadStart > data.length)
          throw new IOException("Simple8bCodec: malformed data (null bitmap exceeds buffer, size=" + data.length + ")");
        buf = ByteBuffer.wrap(data, (int) payloadStart, data.length - (int) payloadStart).slice();
        totalCount = buf.getInt();
        if (totalCount == NULLABLE_MARKER || (totalCount >= 0 ? totalCount : -(long) totalCount) != count)
          throw new IOException("Simple8bCodec: the nullable header declares " + count + " value(s) but its payload " + totalCount);
      }
      if (totalCount < 0) {
        // Raw escape layout (issue #9309). Integer.MIN_VALUE has no positive counterpart: malformed
        if (totalCount == Integer.MIN_VALUE)
          throw new IOException("Simple8bCodec: invalid count " + totalCount + " in header");
        // Checked before allocating: a corrupt header must not request an array the buffer cannot fill
        if ((long) -totalCount * Long.BYTES > buf.remaining())
          throw new IOException("Simple8bCodec: malformed data (raw count " + -totalCount + " exceeds buffer, size=" + data.length + ")");
        final long[] raw = new long[-totalCount];
        for (int i = 0; i < raw.length; i++)
          raw[i] = buf.getLong();
        return raw;
      }
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
    } catch (final BufferUnderflowException e) {
      throw new IOException("Simple8bCodec: malformed data (truncated buffer, size=" + data.length + ")", e);
    }
  }

  private static long zigzagEncode(final long n) {
    return (n << 1) ^ (n >> 63);
  }

  private static long zigzagDecode(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }
}
