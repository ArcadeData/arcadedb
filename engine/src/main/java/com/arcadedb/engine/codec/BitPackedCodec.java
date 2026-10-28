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
package com.arcadedb.engine.codec;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Random-access bit-packed codec for arrays of signed longs. Values are stored as
 * {@code q[i] = (v[i] - min) / gcd} in {@code w} bits each, where {@code w} is the width of the largest quotient. All
 * delta arithmetic wraps and is read as unsigned, so the full {@code long} range (including {@code Long.MIN_VALUE} and
 * {@code Long.MAX_VALUE} in one array) is supported. A point read touches at most two 64-bit words.
 * <p>
 * Layout, little-endian: id(1) width(1) reserved(2) count(4) min(8) gcd(8) then the packed words, value {@code i} at bit
 * {@code i * w}.
 */
public final class BitPackedCodec {
  public static final byte CODEC_ID     = 1;
  public static final int  HEADER_BYTES = 24;

  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE  = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private BitPackedCodec() {
  }

  public static byte[] encode(final long[] values, final int offset, final int length) {
    final byte[] out = new byte[encodedSize(values, offset, length)];
    encodeInto(values, offset, length, out, 0);
    return out;
  }

  public static void decode(final byte[] encoded, final long[] out) {
    decode(encoded, 0, out, 0);
  }

  public static long get(final byte[] encoded, final int index) {
    return get(encoded, 0, index);
  }

  public static int count(final byte[] encoded) {
    return count(encoded, 0);
  }

  static int encodedSize(final long[] values, final int offset, final int length) {
    return sizeForWidth(length, analyze(values, offset, length, 0L, 0.0, null, 0));
  }

  /**
   * Size of the encoding of the residuals {@code values[offset + i] - LinearCodec.predict(intercept, slope, i)}, computed
   * with the same single scan and no allocation.
   */
  static int encodedSizeOfResiduals(final long[] values, final int offset, final int length, final long intercept,
      final double slope) {
    return sizeForWidth(length, analyze(values, offset, length, intercept, slope, null, 0));
  }

  static void encodeInto(final long[] values, final int offset, final int length, final byte[] out, final int base) {
    final int w = analyze(values, offset, length, 0L, 0.0, out, base);
    if (w == 0)
      return;

    final long min = (long) LONG_LE.get(out, base + 8);
    final long gcd = (long) LONG_LE.get(out, base + 16);
    int pos = base + HEADER_BYTES;
    long cur = 0;
    int used = 0;
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      final long q = gcd == 1 ? values[i] - min : Long.divideUnsigned(values[i] - min, gcd);
      cur |= q << used;
      final int total = used + w;
      if (total >= 64) {
        LONG_LE.set(out, pos, cur);
        pos += 8;
        cur = used == 0 ? 0 : q >>> (64 - used);
        used = total - 64;
      } else
        used = total;
    }
    if (used > 0)
      LONG_LE.set(out, pos, cur);
  }

  static long get(final byte[] encoded, final int base, final int index) {
    final int count = (int) INT_LE.get(encoded, base + 4);
    if (index < 0 || index >= count)
      throw new IndexOutOfBoundsException("Index " + index + " out of bounds for count " + count);
    final long min = (long) LONG_LE.get(encoded, base + 8);
    final int w = encoded[base + 1];
    if (w == 0)
      return min;
    final long gcd = (long) LONG_LE.get(encoded, base + 16);
    final long bit = (long) index * w;
    final int word = (int) (bit >>> 6);
    final int shift = (int) (bit & 63);
    final int pos = base + HEADER_BYTES + word * 8;
    long q = (long) LONG_LE.get(encoded, pos) >>> shift;
    if (shift + w > 64)
      q |= (long) LONG_LE.get(encoded, pos + 8) << (64 - shift);
    if (w < 64)
      q &= (1L << w) - 1;
    return min + gcd * q;
  }

  static void decode(final byte[] encoded, final int base, final long[] out, final int outOffset) {
    final int count = count(encoded, base);
    final int w = encoded[base + 1];
    if (w < 0 || w > 64)
      throw new IllegalArgumentException("Invalid bit width " + w);
    if (count == 0)
      return;
    final long words = wordCount(count, w);
    if ((long) encoded.length - base - HEADER_BYTES < words * 8)
      throw new IllegalArgumentException("Truncated bit-packed block");
    if (outOffset < 0 || out.length - outOffset < count)
      throw new IllegalArgumentException("Output array too short: " + (out.length - outOffset) + " < " + count);
    final long min = (long) LONG_LE.get(encoded, base + 8);
    if (w == 0) {
      Arrays.fill(out, outOffset, outOffset + count, min);
      return;
    }
    final long gcd = (long) LONG_LE.get(encoded, base + 16);
    final long mask = w == 64 ? -1L : (1L << w) - 1;
    final int nWords = (int) words;
    int pos = base + HEADER_BYTES;
    int word = 0;
    int shift = 0;
    long cur = (long) LONG_LE.get(encoded, pos);
    for (int i = 0; i < count; i++) {
      long q = cur >>> shift;
      final int end = shift + w;
      if (end >= 64) {
        word++;
        pos += 8;
        cur = word < nWords ? (long) LONG_LE.get(encoded, pos) : 0L;
        if (end > 64)
          q |= cur << (64 - shift);
        shift = end - 64;
      } else
        shift = end;
      out[outOffset + i] = min + gcd * (q & mask);
    }
  }

  static int count(final byte[] encoded, final int base) {
    if (base < 0 || encoded.length - base < HEADER_BYTES)
      throw new IllegalArgumentException("Truncated bit-packed header");
    if (encoded[base] != CODEC_ID)
      throw new IllegalArgumentException("Not a bit-packed block, codec id " + (encoded[base] & 0xFF));
    final int count = (int) INT_LE.get(encoded, base + 4);
    if (count < 0)
      throw new IllegalArgumentException("Invalid count " + count);
    return count;
  }

  static void checkSlice(final long[] values, final int offset, final int length) {
    if (offset < 0 || length < 0 || (long) offset + length > values.length)
      throw new IndexOutOfBoundsException("Slice [" + offset + ", +" + length + ") outside array of " + values.length);
  }

  /** Greatest common divisor of two unsigned 64-bit values; {@code gcd(0, x) = x}. */
  static long unsignedGcd(long a, long b) {
    if (a == 0)
      return b;
    if (b == 0)
      return a;
    final int shift = Long.numberOfTrailingZeros(a | b);
    a >>>= Long.numberOfTrailingZeros(a);
    do {
      b >>>= Long.numberOfTrailingZeros(b);
      if (Long.compareUnsigned(a, b) > 0) {
        final long t = a;
        a = b;
        b = t;
      }
      b -= a;
    } while (b != 0);
    return a << shift;
  }

  /** Bit width of the quotients {@code (v - min) / gcd} for a slice with the given statistics. */
  static int width(final long min, final long max, final long gcd) {
    final long range = max - min;
    return 64 - Long.numberOfLeadingZeros(gcd == 1 ? range : Long.divideUnsigned(range, gcd));
  }

  /** Size in bytes of an encoding of {@code count} values at bit width {@code w}. */
  static int sizeForWidth(final int count, final int w) {
    return Math.toIntExact(HEADER_BYTES + wordCount(count, w) * 8L);
  }

  private static long wordCount(final int count, final int w) {
    return ((long) count * w + 63) >>> 6;
  }

  /**
   * The only analysis of a slice (or of its residuals against a line, plain values being the line 0 + 0.0 * i): one
   * pass finds min, max and the gcd of the differences from the first value (equal to the gcd of the differences from
   * the min, with an early exit once it is 1). Returns the bit width of the quotients;
   * when {@code header} is not null it also writes the header fields (id, width, reserved, count, min, gcd) at
   * {@code base}.
   */
  private static int analyze(final long[] values, final int offset, final int length, final long intercept,
      final double slope, final byte[] header, final int base) {
    checkSlice(values, offset, length);
    long min = 0;
    long gcd = 1;
    int w = 0;
    if (length > 0) {
      final long v0 = values[offset] - LinearCodec.predict(intercept, slope, 0);
      long max = v0;
      long g = 0;
      min = v0;
      final int end = offset + length;
      int i = offset + 1;
      for (; i < end && g != 1; i++) {
        final long v = values[i] - LinearCodec.predict(intercept, slope, i - offset);
        if (v < min)
          min = v;
        else if (v > max)
          max = v;
        g = unsignedGcd(g, v >= v0 ? v - v0 : v0 - v);
      }
      for (; i < end; i++) {
        final long v = values[i] - LinearCodec.predict(intercept, slope, i - offset);
        if (v < min)
          min = v;
        else if (v > max)
          max = v;
      }
      gcd = g == 0 ? 1 : g;
      w = width(min, max, gcd);
    }
    if (header != null) {
      header[base] = CODEC_ID;
      header[base + 1] = (byte) w;
      header[base + 2] = 0;
      header[base + 3] = 0;
      INT_LE.set(header, base + 4, length);
      LONG_LE.set(header, base + 8, min);
      LONG_LE.set(header, base + 16, gcd);
    }
    return w;
  }
}
