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
 * A window offered no sample at all and one whose samples were all absent get the SAME answer, {@link #ABSENT},
 * and no accumulator tells them apart (issue #7694). Issue #7506 briefly seeded SUM at the additive identity so
 * that they could be: no query reaches that state - see {@code MultiColumnAggregationResult.newInitializedValues}
 * for which paths rule it out - and it disagreed with the SQL semantics this policy is modelled on, where
 * {@code SUM} over an empty group and over an all-NULL group are both NULL. {@code COUNT} remains the one
 * aggregate that answers the number zero for no rows.
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

  /**
   * The bits of Prometheus' staleness marker ({@code value.StaleNaN}, issue #11991): the NaN a remote write sends when a series
   * disappears. It is a NaN, so every aggregate of this policy skips it like any other, and only PromQL tells it apart, by its
   * bits: a series whose newest sample is the marker has ENDED. Every layer that stores a floating-point value therefore has to
   * hand these bits back as they came (issue #12580): the mutable row and {@code GORILLA_XOR} keep a double's raw bits, and
   * {@link #STALE_MARKER_FLOAT_BITS} and {@link #STALE_MARKER_TEXT} are its forms where a double is not kept as such.
   */
  public static final long   STALE_MARKER_BITS       = 0x7ff0000000000002L;
  /** The staleness marker as a double. Only its bits tell it from {@link #ABSENT}: test it with {@link #isStaleMarker(double)}. */
  public static final double STALE_MARKER            = Double.longBitsToDouble(STALE_MARKER_BITS);
  /**
   * The staleness marker in a FLOAT column (issue #12580). Narrowing the double drops the payload that makes it the marker, so
   * {@link #toFloat(double)} maps it to this quiet NaN, which every platform moves without touching, and {@link #toDouble(float)}
   * maps it back.
   */
  public static final int    STALE_MARKER_FLOAT_BITS = 0x7fc00002;
  public static final float  STALE_MARKER_FLOAT      = Float.intBitsToFloat(STALE_MARKER_FLOAT_BITS);
  /**
   * The staleness marker in a column sealed as text, the {@code DICTIONARY} codec (issue #12580), where the double's own text
   * would be {@code NaN} and read back as an ordinary NaN. The name of the marker in Prometheus; a reader that does not know it
   * hands the text back as it is rather than failing, as it does for any unparseable entry.
   */
  public static final String STALE_MARKER_TEXT       = "StaleNaN";

  private TimeSeriesNaN() {
  }

  /**
   * Whether {@code value} is the absent marker rather than data.
   */
  public static boolean isAbsent(final double value) {
    return Double.isNaN(value);
  }

  /** Whether {@code value} is the staleness marker, by its bits: an ordinary NaN is not. */
  public static boolean isStaleMarker(final double value) {
    return Double.doubleToRawLongBits(value) == STALE_MARKER_BITS;
  }

  /** Whether {@code value} is the staleness marker as a FLOAT column holds it ({@link #STALE_MARKER_FLOAT_BITS}). */
  public static boolean isStaleMarker(final float value) {
    return Float.floatToRawIntBits(value) == STALE_MARKER_FLOAT_BITS;
  }

  /** Whether a boxed value is the staleness marker, as a DOUBLE or as a FLOAT holds it. */
  public static boolean isStaleMarker(final Object value) {
    if (value instanceof Double d)
      return isStaleMarker(d.doubleValue());
    return value instanceof Float f && isStaleMarker(f.floatValue());
  }

  /** Narrows a double for a FLOAT column, keeping the staleness marker one (issue #12580). */
  public static float toFloat(final double value) {
    return isStaleMarker(value) ? STALE_MARKER_FLOAT : (float) value;
  }

  /** Widens the value of a FLOAT column, giving the staleness marker its double bits back (issue #12580). */
  public static double toDouble(final float value) {
    return isStaleMarker(value) ? STALE_MARKER : value;
  }

  /** {@link #toFloat(double)} for a boxed number: a {@link Float} is already one. */
  public static float floatOf(final Number value) {
    return value instanceof Float f ? f : toFloat(value.doubleValue());
  }

  /** {@link #toDouble(float)} for a boxed number: any other number widens as it always did. */
  public static double doubleOf(final Number value) {
    return value instanceof Float f ? toDouble(f) : value.doubleValue();
  }

  /**
   * The text a value is sealed as in a {@code DICTIONARY} column: the empty text for a sample with no value, the staleness marker
   * as {@link #STALE_MARKER_TEXT} (issue #12580), anything else as its own text. Every writer of such a column and of the distinct
   * values a block declares for it goes through here, so the declaration stays the dictionary.
   */
  public static String dictionaryText(final Object value) {
    if (value == null)
      return "";
    if (isStaleMarker(value))
      return STALE_MARKER_TEXT;
    return value.toString();
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
   * {@code null} is {@link #ABSENT}: it is what a row hands back for a sample that carries no value. A floating-point
   * column never hands one back - it stores the NaN itself, so a null measurement arrives here as a NaN {@link Double} -
   * and since issue #9721 an integral column of a type whose rows carry a null bitmap does, on both layers: the mutable
   * row reads its bit, and a sealed block's {@code SIMPLE8B} column carries the same bitmap. A type created before that
   * format still stores a null integral value as a real zero, which arrives here as the zero it is.
   * <p>
   * The remaining arm - a value that is neither null, a boolean nor a number - is a column the sealed layer
   * cannot read as a number at all ({@code DICTIONARY} and {@code DELTA_OF_DELTA} have no numeric decoder, and
   * {@code decompressDoubleColumnFromBytes} throws on them). Such a request is refused before it reaches either
   * layer, by {@link TimeSeriesGateway#requireAggregatableColumn}; this answers {@link #ABSENT} so that a path
   * that ever slips past the refusal reports a gap rather than inventing a zero.
   */
  public static double asMeasurement(final Object value) {
    if (value == null)
      return ABSENT;
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
   * @param present how many real samples the accumulator holds, BEFORE this one - see {@link #countIfPresent}
   */
  public static double sum(final double accumulator, final long present, final double sample) {
    if (Double.isNaN(sample))
      return accumulator;
    return present == 0 ? sample : accumulator + sample;
  }

  /**
   * Merges a partial SUM into a running one: the same rule as {@link #sum(double, long, double)}, with each side's
   * count of real samples saying whether its value is a total or an absence. A partial with no real sample is
   * skipped whatever its value; a running sum with none is replaced; two totals are added, NaN included.
   */
  public static double mergeSum(final double accumulator, final long present, final double partial,
      final long partialPresent) {
    if (partialPresent == 0)
      return accumulator;
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
