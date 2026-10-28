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

/**
 * Codec for arrays of signed longs that follow a line. The line goes through the first and the last value; the
 * residuals {@code v[i] - predict(i)} (wrapping) are stored with {@link BitPackedCodec}, so {@code get(i)} stays O(1).
 * <p>
 * Layout, little-endian: id(1) reserved(7) intercept(8) slope(8, raw double bits) then a complete {@link BitPackedCodec}
 * encoding of the residuals. The prediction is {@code intercept + (long) (slope * i)}; Java floating point is strict,
 * so encoder and decoder compute bit-identical predictions.
 * <p>
 * A line spanning more than half the long range saturates the {@code (long)} cast of the prediction: the data still
 * round-trips exactly, only the residuals (hence the encoding) get larger, and {@code NumericCodecChooser} picks by
 * size so it never prefers such an encoding. {@code get} relies on the JVM array bounds checks instead of validating
 * the payload length, to keep the point read cheap.
 */
public final class LinearCodec {
  public static final byte CODEC_ID     = 2;
  public static final int  HEADER_BYTES = 24;

  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private LinearCodec() {
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
    BitPackedCodec.checkSlice(values, offset, length);
    return HEADER_BYTES + BitPackedCodec.encodedSizeOfResiduals(values, offset, length, length > 0 ? values[offset] : 0L,
        slope(values, offset, length));
  }

  static void encodeInto(final long[] values, final int offset, final int length, final byte[] out, final int base) {
    BitPackedCodec.checkSlice(values, offset, length);
    final long intercept = length > 0 ? values[offset] : 0L;
    final double slope = slope(values, offset, length);
    out[base] = CODEC_ID;
    for (int i = 1; i < 8; i++)
      out[base + i] = 0;
    LONG_LE.set(out, base + 8, intercept);
    LONG_LE.set(out, base + 16, Double.doubleToRawLongBits(slope));

    final long[] residuals = new long[length];
    for (int i = 0; i < length; i++)
      residuals[i] = values[offset + i] - predict(intercept, slope, i);
    BitPackedCodec.encodeInto(residuals, 0, length, out, base + HEADER_BYTES);
  }

  static long get(final byte[] encoded, final int base, final int index) {
    checkHeader(encoded, base);
    final long intercept = (long) LONG_LE.get(encoded, base + 8);
    final double slope = Double.longBitsToDouble((long) LONG_LE.get(encoded, base + 16));
    return BitPackedCodec.get(encoded, base + HEADER_BYTES, index) + predict(intercept, slope, index);
  }

  static void decode(final byte[] encoded, final int base, final long[] out, final int outOffset) {
    checkHeader(encoded, base);
    final int count = BitPackedCodec.count(encoded, base + HEADER_BYTES);
    BitPackedCodec.decode(encoded, base + HEADER_BYTES, out, outOffset);
    final long intercept = (long) LONG_LE.get(encoded, base + 8);
    final double slope = Double.longBitsToDouble((long) LONG_LE.get(encoded, base + 16));
    for (int i = 0; i < count; i++)
      out[outOffset + i] += predict(intercept, slope, i);
  }

  static int count(final byte[] encoded, final int base) {
    checkHeader(encoded, base);
    return BitPackedCodec.count(encoded, base + HEADER_BYTES);
  }

  /** Length of the encoding starting at {@code base}, read from its header in O(1); refuses a malformed header. */
  static int encodedLength(final byte[] encoded, final int base) {
    final int count = count(encoded, base);
    final int w = encoded[base + HEADER_BYTES + 1] & 0xFF;
    if (w > 64)
      throw new IllegalArgumentException("Invalid bit width " + w);
    try {
      return HEADER_BYTES + BitPackedCodec.sizeForWidth(count, w);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException("Invalid count " + count, e);
    }
  }

  static long predict(final long intercept, final double slope, final int i) {
    return intercept + (long) (slope * i);
  }

  private static double slope(final long[] values, final int offset, final int length) {
    return length > 1 ? ((double) values[offset + length - 1] - (double) values[offset]) / (length - 1) : 0.0;
  }

  private static void checkHeader(final byte[] encoded, final int base) {
    if (base < 0 || encoded.length - base < HEADER_BYTES)
      throw new IllegalArgumentException("Truncated linear header");
    if (encoded[base] != CODEC_ID)
      throw new IllegalArgumentException("Not a linear block, codec id " + (encoded[base] & 0xFF));
  }
}
