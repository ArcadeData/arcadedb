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
package com.arcadedb.engine.timeseries;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * An exact sum of {@code long}s as a signed 128-bit integer held in two longs, {@code (lo, hi)}: the low word unsigned, the high
 * word carrying the sign (issue #9718).
 * <p>
 * The TimeSeries aggregation of an integral column used to add its samples as doubles, so a total past 2^53 came back rounded,
 * and one past 2^63 had no {@code long} to come back as. 128 bits hold the sum of 2^64 samples of any {@code long} value, which no
 * block, bucket or query reaches, so a sum folded here is exact whatever it adds up, and a result is turned into a number only
 * once, at the end: a {@link Long} when it fits, a {@link BigDecimal} otherwise - the types the generic SQL {@code sum()} answers.
 * <p>
 * Static helpers over primitive pairs rather than an object, so an accumulator is two slots of a {@code long[]} and folding a
 * sample allocates nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ExactLongSum {

  private static final BigInteger TWO_POW_64 = BigInteger.ONE.shiftLeft(64);

  private ExactLongSum() {
  }

  /**
   * Adds {@code value} to the sum held in {@code acc[at]} (low word) and {@code acc[at + 1]} (high word).
   */
  public static void add(final long[] acc, final int at, final long value) {
    final long lo = acc[at];
    final long sum = lo + value;
    // the carry out of the unsigned low-word addition, plus the sign extension of the value into the high word
    acc[at + 1] += (value >> 63) + (Long.compareUnsigned(sum, lo) < 0 ? 1 : 0);
    acc[at] = sum;
  }

  /**
   * Adds the 128-bit value {@code (lo, hi)} to the sum held in {@code acc[at]}, {@code acc[at + 1]}.
   */
  public static void add(final long[] acc, final int at, final long lo, final long hi) {
    final long accLo = acc[at];
    final long sum = accLo + lo;
    acc[at + 1] += hi + (Long.compareUnsigned(sum, accLo) < 0 ? 1 : 0);
    acc[at] = sum;
  }

  /**
   * Whether the sum {@code (lo, hi)} is a {@code long}: its high word is nothing but the sign of its low word.
   */
  public static boolean fitsLong(final long lo, final long hi) {
    return hi == (lo >> 63);
  }

  /**
   * The sum as a {@link Long} when it fits one, else as an exact {@link BigDecimal} - what the generic {@code sum()} answers
   * for the same samples, which widens to a {@code BigDecimal} on overflow (see {@code Type.increment}).
   */
  public static Number toNumber(final long lo, final long hi) {
    if (fitsLong(lo, hi))
      return lo;
    return new BigDecimal(toBigInteger(lo, hi));
  }

  /**
   * The sum as the nearest double.
   */
  public static double toDouble(final long lo, final long hi) {
    if (fitsLong(lo, hi))
      return lo;
    return toBigInteger(lo, hi).doubleValue();
  }

  /**
   * The mean of {@code count} samples whose sum is {@code (lo, hi)}, as a double, computed the way the generic {@code avg()}
   * computes it: the total divided by the count, through a decimal division when the total is past the {@code long} range.
   */
  public static double average(final long lo, final long hi, final long count) {
    if (fitsLong(lo, hi))
      return (double) lo / count;
    return new BigDecimal(toBigInteger(lo, hi)).divide(BigDecimal.valueOf(count), MathContext.DECIMAL128).doubleValue();
  }

  /**
   * The mean of {@code count} samples whose sum is {@code (lo, hi)}, truncated toward zero: an integral column's downsampled
   * value, which has to be a sample of the column's type. A mean of {@code long}s is always a {@code long}.
   */
  public static long truncatedAverage(final long lo, final long hi, final long count) {
    if (fitsLong(lo, hi))
      return lo / count;
    return toBigInteger(lo, hi).divide(BigInteger.valueOf(count)).longValue();
  }

  /**
   * The sum {@code (lo, hi)} as a {@link BigInteger}.
   */
  public static BigInteger toBigInteger(final long lo, final long hi) {
    final BigInteger low = lo >= 0 ? BigInteger.valueOf(lo) : BigInteger.valueOf(lo).add(TWO_POW_64);
    return BigInteger.valueOf(hi).shiftLeft(64).add(low);
  }
}
