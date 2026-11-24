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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.engine.timeseries.TimeSeriesNaN;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;

import java.util.List;

/**
 * Static function implementations for PromQL functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PromQLFunctions {

  private PromQLFunctions() {
  }

  /**
   * Whether the range function {@code function} answers anything for {@code series} (issue #11980). Prometheus emits NO sample for
   * a series {@code rate()}/{@code increase()} cannot compute a rate for - fewer than two samples in the window - nor for one whose
   * last two samples {@code irate()} cannot divide by; every other range function answers any series the selector returned.
   * The window's samples are in timestamp order (see {@code evaluateMatrixSelector}).
   * <p>
   * This is the one statement of that rule: {@link #rate}, {@link #increase} and {@link #irate} ask it too, and answer
   * {@link TimeSeriesNaN#ABSENT} - never a {@code 0} nobody measured - to a caller that did not.
   */
  public static boolean answers(final String function, final RangeSeries series) {
    return switch (function) {
      case "rate", "increase" -> hasSampledInterval(series.values());
      case "irate" -> hasInstantInterval(series.values());
      default -> true;
    };
  }

  /** {@code rate()}/{@code increase()}: at least two samples, the first and the last at different times. */
  private static boolean hasSampledInterval(final List<double[]> values) {
    return values.size() >= 2 && values.getLast()[0] > values.getFirst()[0];
  }

  /** {@code irate()}: at least two samples, the last two at different times. */
  private static boolean hasInstantInterval(final List<double[]> values) {
    return values.size() >= 2 && values.getLast()[0] > values.get(values.size() - 2)[0];
  }

  /**
   * Per-second rate of increase, with counter-reset handling.
   * <p>
   * Extrapolates the sampled increase to the full matrix range window, matching Prometheus
   * {@code rate()} semantics: the result is normalized over the requested range (e.g. {@code [5m]})
   * rather than over the actual span between the first and last sample. Without this, sparse data
   * (few samples spread across a wide window) would over-estimate the rate.
   */
  public static double rate(final RangeSeries series) {
    return extrapolatedRate(series, true);
  }

  /**
   * Instant rate from the last two points.
   */
  public static double irate(final RangeSeries series) {
    final List<double[]> values = series.values();
    if (!hasInstantInterval(values))
      return TimeSeriesNaN.ABSENT;
    final double[] prev = values.get(values.size() - 2);
    final double[] last = values.getLast();
    final double durationSec = (last[0] - prev[0]) / 1000.0;
    double diff = last[1] - prev[1];
    if (diff < 0)
      diff = last[1]; // counter reset
    return diff / durationSec;
  }

  /**
   * Total increase over the range, with counter-reset handling.
   * <p>
   * Extrapolates the sampled increase to the full matrix range window, matching Prometheus
   * {@code increase()} semantics (which is defined as {@code rate() * range}). Without this, sparse
   * data would under-count the increase relative to the requested window.
   */
  public static double increase(final RangeSeries series) {
    return extrapolatedRate(series, false);
  }

  /**
   * Shared extrapolation logic for {@code rate()} and {@code increase()}, faithfully porting
   * Prometheus' {@code extrapolatedRate} as it is since Prometheus 2.51 (prometheus/prometheus#13725, checked against
   * v3.5.0 {@code promql/functions.go}). The counter-reset-corrected total increase is scaled so it
   * covers the full requested matrix window, with boundary handling that avoids over-extrapolating
   * beyond the available samples and prevents extrapolating a counter below zero.
   *
   * @param isRate when {@code true} the result is divided by the range duration to yield a per-second
   *               rate ({@code rate()}); when {@code false} the absolute extrapolated increase is
   *               returned ({@code increase()}).
   */
  private static double extrapolatedRate(final RangeSeries series, final boolean isRate) {
    final List<double[]> values = series.values();
    if (!hasSampledInterval(values))
      return TimeSeriesNaN.ABSENT;

    final double[] first = values.getFirst();
    final double[] last = values.getLast();

    // Counter-reset-corrected total increase across the sampled points.
    double resultValue = 0;
    double prev = first[1];
    for (int i = 1; i < values.size(); i++) {
      final double current = values.get(i)[1];
      if (current < prev)
        resultValue += current; // counter reset
      else
        resultValue += current - prev;
      prev = current;
    }

    final double sampledInterval = (last[0] - first[0]) / 1000.0;

    final double rangeStartSec = series.rangeStartMs() / 1000.0;
    final double rangeEndSec = series.rangeEndMs() / 1000.0;
    final double rangeSec = rangeEndSec - rangeStartSec;
    if (rangeSec <= 0)
      return TimeSeriesNaN.ABSENT;

    double durationToStart = (first[0] / 1000.0) - rangeStartSec;
    double durationToEnd = rangeEndSec - (last[0] / 1000.0);
    final double averageDurationBetweenSamples = sampledInterval / (values.size() - 1);

    // A boundary farther than 10% past the average spacing means the series starts or ends inside the window: extrapolate
    // only half a spacing toward it.
    final double extrapolationThreshold = averageDurationBetweenSamples * 1.1;
    if (durationToStart >= extrapolationThreshold)
      durationToStart = averageDurationBetweenSamples / 2;

    // Counters can't be negative. If the counter has positive slope and a non-negative first value, extrapolate back only as far
    // as the projected zero point. AFTER the threshold, as Prometheus does since 2.51 (prometheus/prometheus#13725): the zero
    // point used to be applied first, and a zero point under the threshold then extrapolated past the half spacing the series
    // start had already been limited to (issue #11981)
    if (resultValue > 0 && first[1] >= 0) {
      final double durationToZero = sampledInterval * (first[1] / resultValue);
      if (durationToZero < durationToStart)
        durationToStart = durationToZero;
    }

    if (durationToEnd >= extrapolationThreshold)
      durationToEnd = averageDurationBetweenSamples / 2;

    final double extrapolateToInterval = sampledInterval + durationToStart + durationToEnd;

    double factor = extrapolateToInterval / sampledInterval;
    if (isRate)
      factor /= rangeSec;

    return resultValue * factor;
  }

  public static double sumOverTime(final RangeSeries series) {
    double sum = 0;
    for (final double[] v : series.values())
      sum += v[1];
    return sum;
  }

  public static double avgOverTime(final RangeSeries series) {
    if (series.values().isEmpty())
      return 0.0;
    return sumOverTime(series) / series.values().size();
  }

  /**
   * Prometheus answers NaN for {@code min_over_time} over a window with no real sample, so the fold goes through
   * {@link TimeSeriesNaN} - the one NaN policy of the time-series stack - instead of seeding {@code +Infinity} and
   * returning that sentinel as data when nothing displaced it (issue #7039).
   */
  public static double minOverTime(final RangeSeries series) {
    double min = TimeSeriesNaN.ABSENT;
    for (final double[] v : series.values())
      min = TimeSeriesNaN.min(min, v[1]);
    return min;
  }

  /**
   * See {@link #minOverTime(RangeSeries)}.
   */
  public static double maxOverTime(final RangeSeries series) {
    double max = TimeSeriesNaN.ABSENT;
    for (final double[] v : series.values())
      max = TimeSeriesNaN.max(max, v[1]);
    return max;
  }

  public static double countOverTime(final RangeSeries series) {
    return series.values().size();
  }

  public static double abs(final double value) {
    return Math.abs(value);
  }

  public static double ceil(final double value) {
    return Math.ceil(value);
  }

  public static double floor(final double value) {
    return Math.floor(value);
  }

  public static double round(final double value, final double toNearest) {
    if (toNearest == 0)
      return value;
    // Stays in double like Prometheus (issue #8928): Math.round(double) returns a long, which turns NaN into 0
    // and saturates infinities and anything above 2^63.
    final double toNearestInverse = 1.0 / toNearest;
    return Math.floor(value * toNearestInverse + 0.5) / toNearestInverse;
  }
}
