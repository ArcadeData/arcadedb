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

/**
 * THE NaN policy for the whole time-series stack: {@code NaN} is the ABSENT marker, and every aggregate skips it.
 * <p>
 * The policy exists as one class because the subsystem previously carried three of them. Every MIN/MAX
 * accumulator started from a sentinel of its own choosing - {@code ±Infinity} in the PromQL functions and the
 * instant-vector aggregation arms, {@code ±Double.MAX_VALUE} in the multi-column accumulator, the block-statistics
 * reducer's {@code ±Double.MAX_VALUE} on disk - and each one leaked that sentinel back to the caller as though it
 * were data whenever the window it summarised held no real value (issues #4596, #4716, #7039, #7043). Two paths
 * over the same all-NaN samples answered differently, which is what made the leak more than cosmetic.
 * <p>
 * The fix removes the sentinels rather than guarding them: seed a MIN/MAX accumulator with {@link #ABSENT} and
 * fold with {@link #min(double, double)} / {@link #max(double, double)}. A NaN sample never displaces the
 * accumulator and the accumulator is NaN if and only if nothing real ever reached it, so "no data" needs no
 * side-channel (a count, a bitset, a {@code found} flag) to be told apart from a real minimum. Merging two
 * partial results is the same fold, so it inherits the property for free.
 * <p>
 * SUM and AVG follow the same rule (issue #7089): a NaN sample is not a measurement of "not a number", it is the
 * absence of a measurement, and a sum over 999 real samples and one absent one is the sum of the 999 - the way SQL's
 * {@code SUM} and {@code AVG} skip NULL. So {@link #sum(double, double)} is the same fold shape, {@code AVG} divides
 * the folded sum by the number of samples that were REAL ({@link #countIfPresent(long, double)}), and a window
 * whose every sample was absent answers {@link #ABSENT} for all four, rather than a NaN that merely looks the same
 * but was produced by IEEE arithmetic poisoning a real total. {@code COUNT} is the one aggregate that does not
 * skip: it counts rows, as SQL's {@code COUNT(*)} does, and the SQL push-down maps it from {@code count(*)}.
 * <p>
 * The PromQL layer is deliberately NOT under this policy for {@code sum}/{@code avg}: Prometheus propagates NaN
 * through those, and a PromQL query is expected to answer what Prometheus would.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesNaN {

  /**
   * The absent marker. A MIN/MAX result equal to this means the window carried no non-NaN sample.
   */
  public static final double ABSENT = Double.NaN;

  private TimeSeriesNaN() {
  }

  /**
   * Whether {@code value} is the absent marker rather than data.
   */
  public static boolean isAbsent(final double value) {
    return Double.isNaN(value);
  }

  /**
   * Folds one sample into a running MIN. {@code accumulator} starts at {@link #ABSENT}; a NaN sample is skipped,
   * and the first real sample replaces the absent accumulator outright.
   */
  public static double min(final double accumulator, final double sample) {
    if (Double.isNaN(sample))
      return accumulator;
    return Double.isNaN(accumulator) || sample < accumulator ? sample : accumulator;
  }

  /**
   * Folds one sample into a running MAX. See {@link #min(double, double)}.
   */
  public static double max(final double accumulator, final double sample) {
    if (Double.isNaN(sample))
      return accumulator;
    return Double.isNaN(accumulator) || sample > accumulator ? sample : accumulator;
  }

  /**
   * Folds one sample into a running SUM (issue #7089). {@code accumulator} starts at {@link #ABSENT}; a NaN sample
   * is skipped, the first real sample replaces the absent accumulator outright, and every later one is added.
   * The accumulator is therefore NaN if and only if no real sample ever reached it - never because one of them was
   * NaN, which is what a plain {@code +=} produces. Merging two partial sums is the same fold.
   */
  public static double sum(final double accumulator, final double sample) {
    if (Double.isNaN(sample))
      return accumulator;
    return Double.isNaN(accumulator) ? sample : accumulator + sample;
  }

  /**
   * The count of REAL samples after {@code sample} reached the accumulator: unchanged for a NaN sample, one more
   * otherwise. This is the denominator of AVG under the policy - the count of samples that contributed to the
   * sum - and it is what tells an all-absent window apart from one that summed to zero.
   */
  public static long countIfPresent(final long count, final double sample) {
    return Double.isNaN(sample) ? count : count + 1;
  }
}
