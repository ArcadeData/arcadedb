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
 * {@code SUM} and {@code AVG} skip NULL. So {@link #sum(double, long, double)} is the same fold shape, {@code AVG}
 * divides the folded sum by the number of samples that were REAL ({@link #countIfPresent(long, double)}), and a
 * window whose every sample was absent answers {@link #ABSENT} for all four. A NaN the arithmetic itself produces
 * over real samples ({@code +Infinity + -Infinity}) is a different thing - an undefined total, kept as IEEE keeps
 * it - which is why the SUM fold is keyed on that count rather than on the accumulator's value. {@code COUNT} is the one aggregate that does not
 * skip: it counts rows, as SQL's {@code COUNT(*)} does, and the SQL push-down maps it from {@code count(*)}.
 * <p>
 * A window that was offered no sample at all is a different question from one whose samples were all absent, and
 * only SUM answers the two differently: the empty sum is the additive identity, so a SUM accumulator seeded with
 * zero and never offered anything stays zero, while one absent sample is enough to make it {@link #ABSENT}
 * (issue #7506).
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
   * The double a value read from the MUTABLE layer contributes to an aggregate, unboxed exactly the way the
   * SEALED layer's codecs unbox the same value on the way in (issue #7725).
   * <p>
   * The mutable and the sealed layer answer the same samples, so they have to answer them alike: whether a
   * sample has been compacted yet is not a question a caller asked. The two used to differ in two ways, both
   * silent. A {@link Boolean} is not a {@link Number} - {@code BOOLEAN} has been an integer column since issue
   * #5475 - so the mutable path read every boolean sample as a real {@code 0.0} while the sealed path, whose
   * {@code SIMPLE8B} column went through {@link ColumnDefinition#integerValueOf(Object)}, read the same samples
   * as 1 and 0. And anything else that is not a measurement became a real {@code 0.0} as well, which under this
   * class's policy is a MEASUREMENT of zero: it enters the SUM and drags the AVG toward it, where
   * {@link #ABSENT} would have been skipped by every aggregate.
   * <p>
   * {@code null} is zero rather than absent on purpose, and that is not a policy choice made here: it is what
   * the sealed layer stores for it. Both {@link ColumnDefinition#integerValueOf(Object)} and
   * {@link ColumnDefinition#numericValueOf(Object)} write a null out as zero, so a null sample reads back as a
   * real zero once compacted, and the mutable layer answering {@link #ABSENT} for it would recreate the very
   * disagreement this method exists to remove.
   * <p>
   * The remaining arm - a value that is neither null, a boolean nor a number - is a column the sealed layer
   * cannot read as a number at all ({@code DICTIONARY} and {@code DELTA_OF_DELTA} have no numeric decoder, and
   * {@code decompressDoubleColumnFromBytes} throws on them). Such a request is refused before it reaches either
   * layer, by {@link TimeSeriesGateway#requireAggregatableColumn}; this answers {@link #ABSENT} so that a path
   * that ever slips past the refusal reports a gap rather than inventing a zero.
   */
  public static double asMeasurement(final Object value) {
    if (value == null)
      return 0.0;
    if (value instanceof final Number n)
      return n.doubleValue();
    if (value instanceof final Boolean b)
      return b ? 1.0 : 0.0;
    return ABSENT;
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
   * <p>
   * "First" is decided by {@code present}, the count of real samples folded so far, and NOT by the accumulator
   * being NaN: a sum can turn NaN by arithmetic - {@code +Infinity + -Infinity} - after real samples reached it,
   * and that NaN is an answer ("the total is undefined"), not an absence. Keying on the accumulator would let the
   * next real sample overwrite it, and would make this fold disagree with the vectorized reduction, which keeps
   * it. So the accumulator is absent if and only if {@code present} is zero, and NaN with {@code present} above
   * zero is the arithmetic result, kept as IEEE keeps it.
   *
   * <p>
   * An absent sample reaching an accumulator that holds no real sample yet returns {@link #ABSENT} rather than the
   * accumulator, so that a caller seeding with the additive identity can still tell "this accumulator was never
   * offered a sample" from "it was offered samples and none was real" (issue #7506). For the callers that seed with
   * {@link #ABSENT} - every one of them before #7506 - this is the accumulator, and the fold is unchanged.
   *
   * @param present how many real samples the accumulator holds, BEFORE this one - see {@link #countIfPresent}
   */
  public static double sum(final double accumulator, final long present, final double sample) {
    if (Double.isNaN(sample))
      return present == 0 ? ABSENT : accumulator;
    return present == 0 ? sample : accumulator + sample;
  }

  /**
   * Merges a partial SUM into a running one: the same rule as {@link #sum(double, long, double)}, with each side's
   * count of real samples saying whether its value is a total or an absence. A partial with no real sample is
   * skipped whatever its value; a running sum with none is replaced; two totals are added, NaN included.
   * <p>
   * When NEITHER side holds a real sample there is no total to protect, and the two "no data" values are added
   * instead of one being dropped: absence propagates through the addition ({@code 0 + NaN} is NaN), so untouched
   * merged with untouched stays the additive identity while untouched merged with absent becomes absent (issue
   * #7506). Both sides are {@link #ABSENT} for every caller that seeds with it, which makes this the same NaN the
   * old branch returned.
   */
  public static double mergeSum(final double accumulator, final long present, final double partial,
      final long partialPresent) {
    if (partialPresent == 0)
      return present == 0 ? accumulator + partial : accumulator;
    return present == 0 ? partial : accumulator + partial;
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
