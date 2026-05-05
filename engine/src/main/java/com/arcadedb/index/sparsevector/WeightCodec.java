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
package com.arcadedb.index.sparsevector;

/**
 * Per-block weight quantization codecs.
 * <p>
 * <b>int8</b>: 254 levels (0..253), packed into a single byte. Byte {@code 0x80} is reserved
 * as a tombstone sentinel, so encoded levels {@code >= 128} get bumped by one to skip it.
 * Decoded weight is {@code weight_min + (level / 253) * (weight_max - weight_min)}, so the
 * end-points of the per-block range are exactly representable.
 * <p>
 * <b>fp16</b>: IEEE 754 half-precision. Sentinel {@code 0xFE00} marks tombstones (a NaN
 * payload outside the legal NaN range used by ordinary fp16 conversions).
 * <p>
 * <b>fp32</b>: native float. Sentinel {@code NaN with bit-pattern 0x7FC0_DEAD} marks
 * tombstones (a quiet-NaN with a payload that no normal computation produces).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class WeightCodec {
  /** Bit pattern used as a tombstone sentinel for fp32 weights. Quiet NaN with a recognizable payload. */
  public static final int FP32_TOMBSTONE_BITS = 0x7FC0DEAD;

  // ---------- int8 ----------

  /**
   * Quantize a weight to a byte using per-block dynamic range. {@code min} and {@code max}
   * are inclusive bounds for the block; weights outside this range are clamped (a violation
   * caller-side, but we don't throw to keep compaction streams robust).
   */
  public static byte quantizeInt8(final float weight, final float min, final float max) {
    if (Float.isNaN(weight))
      throw new IllegalArgumentException("NaN weight is not supported by int8 quantization");
    if (max <= min)
      // Degenerate range: the whole block has a constant weight. Encode level 0 (== min).
      return encodeInt8Level(0);

    final float clamped = weight < min ? min : (weight > max ? max : weight);
    final int level = Math.round((SegmentFormat.INT8_LEVELS - 1) * (clamped - min) / (max - min));
    return encodeInt8Level(level);
  }

  /** Inverse of {@link #quantizeInt8}. Returns {@link Float#NaN} on the tombstone byte. */
  public static float dequantizeInt8(final byte b, final float min, final float max) {
    if (b == SegmentFormat.INT8_TOMBSTONE_SENTINEL)
      return Float.NaN;
    final int level = decodeInt8Level(b);
    if (max <= min)
      return min;
    return min + (level / (float) (SegmentFormat.INT8_LEVELS - 1)) * (max - min);
  }

  /**
   * Per-posting worst-case quantization error for the given block range:
   * {@code (max - min) / (2 * (LEVELS - 1))} = {@code (max - min) / 506} at {@code LEVELS = 254}.
   * <p>
   * <b>Aggregate score-error contract.</b> Top-K scoring sums {@code queryWeight * postingWeight}
   * over the query's {@code nnz} dims, so the worst-case absolute error on a single document's
   * score is bounded by {@code sum_i |q_i| * (max_i - min_i) / 506}. With unit-normalized query
   * weights and a typical {@code max - min} of ~1.0 across a block, this is {@code ~nnz / 506}
   * per document. Test tolerances on top-K score equality are sized against this bound (see
   * {@code LSMSparseVectorIndexTest} - {@code 5e-3} is comfortably above it for {@code nnz <= 30}
   * with non-unit ranges).
   */
  public static float maxQuantizationError(final float min, final float max) {
    if (max <= min)
      return 0.0f;
    return (max - min) / (2.0f * (SegmentFormat.INT8_LEVELS - 1));
  }

  private static byte encodeInt8Level(final int level) {
    final int clamped = level < 0 ? 0 : (level > SegmentFormat.INT8_LEVELS - 1 ? SegmentFormat.INT8_LEVELS - 1 : level);
    // Skip 0x80 (-128 signed) which is reserved as the tombstone sentinel: levels [0..127] map to bytes [0x00..0x7F],
    // levels [128..253] map to bytes [0x81..0xFE].
    return (byte) (clamped < 0x80 ? clamped : clamped + 1);
  }

  private static int decodeInt8Level(final byte b) {
    final int u = b & 0xFF;
    if (u == 0x80)
      throw new IllegalStateException("0x80 is reserved as tombstone sentinel and must not be decoded as a weight");
    return u < 0x80 ? u : u - 1;
  }

  // ---------- fp16 ----------

  /**
   * IEEE 754 half-precision float encoding. Returns a 16-bit value packed in a short.
   * Round-to-nearest-even, subnormal-preserving.
   */
  public static short toFp16(final float f) {
    final int bits = Float.floatToRawIntBits(f);
    final int sign = (bits >>> 16) & 0x8000;
    int val = (bits & 0x7FFFFFFF) + 0x1000;  // round
    if (val >= 0x47800000) {
      // Inf or NaN
      if ((bits & 0x7FFFFFFF) >= 0x7F800000)
        return (short) (sign | 0x7C00 | ((bits & 0x007FFFFF) >>> 13));
      return (short) (sign | 0x7BFF);  // saturate to max finite
    }
    if (val >= 0x38800000)
      return (short) (sign | ((val - 0x38000000) >>> 13));
    if (val < 0x33000000)
      return (short) sign;  // underflow to zero
    val = (bits & 0x7FFFFFFF) >>> 23;
    return (short) (sign | ((((bits & 0x7FFFFF) | 0x800000) + (0x800000 >>> (val - 102))) >>> (126 - val)));
  }

  public static float fromFp16(final short fp16) {
    final int h = fp16 & 0xFFFF;
    final int sign = (h & 0x8000) << 16;
    final int exp = (h >>> 10) & 0x1F;
    final int mant = h & 0x3FF;

    if (exp == 0) {
      if (mant == 0)
        return Float.intBitsToFloat(sign);
      // Subnormal: normalize.
      int e = -1;
      int m = mant;
      do {
        e++;
        m <<= 1;
      } while ((m & 0x400) == 0);
      return Float.intBitsToFloat(sign | (((-14 - e + 127) << 23) | ((m & 0x3FF) << 13)));
    }
    if (exp == 0x1F)
      return Float.intBitsToFloat(sign | 0x7F800000 | (mant << 13));
    return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mant << 13));
  }

  // ---------- fp32 ----------

  public static int floatToTombstoneAwareBits(final float f) {
    if (Float.isNaN(f))
      throw new IllegalArgumentException("NaN weights are not supported in fp32 mode (sentinel collision)");
    return Float.floatToRawIntBits(f);
  }

  public static boolean isFp32Tombstone(final int bits) {
    return bits == FP32_TOMBSTONE_BITS;
  }

  private WeightCodec() {
    // utility class
  }
}
