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
 * THE NaN policy for the whole time-series stack: {@code NaN} is the ABSENT marker, and MIN/MAX skip it.
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
}
