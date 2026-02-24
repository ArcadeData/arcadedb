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
   * Per-second rate of increase, with counter-reset handling.
   */
  public static double rate(final RangeSeries series) {
    final List<double[]> values = series.values();
    if (values.size() < 2)
      return 0.0;
    final double[] first = values.getFirst();
    final double[] last = values.getLast();
    final double durationSec = (last[0] - first[0]) / 1000.0;
    if (durationSec <= 0)
      return 0.0;

    double totalIncrease = 0;
    double prev = first[1];
    for (int i = 1; i < values.size(); i++) {
      final double current = values.get(i)[1];
      if (current < prev)
        totalIncrease += current; // counter reset
      else
        totalIncrease += current - prev;
      prev = current;
    }
    return totalIncrease / durationSec;
  }

  /**
   * Instant rate from the last two points.
   */
  public static double irate(final RangeSeries series) {
    final List<double[]> values = series.values();
    if (values.size() < 2)
      return 0.0;
    final double[] prev = values.get(values.size() - 2);
    final double[] last = values.getLast();
    final double durationSec = (last[0] - prev[0]) / 1000.0;
    if (durationSec <= 0)
      return 0.0;
    double diff = last[1] - prev[1];
    if (diff < 0)
      diff = last[1]; // counter reset
    return diff / durationSec;
  }

  /**
   * Total increase over the range, with counter-reset handling.
   */
  public static double increase(final RangeSeries series) {
    final List<double[]> values = series.values();
    if (values.size() < 2)
      return 0.0;
    double totalIncrease = 0;
    double prev = values.getFirst()[1];
    for (int i = 1; i < values.size(); i++) {
      final double current = values.get(i)[1];
      if (current < prev)
        totalIncrease += current; // counter reset
      else
        totalIncrease += current - prev;
      prev = current;
    }
    return totalIncrease;
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

  public static double minOverTime(final RangeSeries series) {
    double min = Double.POSITIVE_INFINITY;
    for (final double[] v : series.values())
      if (v[1] < min)
        min = v[1];
    return min;
  }

  public static double maxOverTime(final RangeSeries series) {
    double max = Double.NEGATIVE_INFINITY;
    for (final double[] v : series.values())
      if (v[1] > max)
        max = v[1];
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
    return Math.round(value / toNearest) * toNearest;
  }
}
