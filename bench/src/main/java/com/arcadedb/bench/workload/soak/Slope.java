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
package com.arcadedb.bench.workload.soak;

import java.util.OptionalDouble;

/**
 * The least-squares slope of a series over time, per hour: how fast a soak's heap floor, thread count or open files grow.
 */
public final class Slope {
  /** Fewer samples than this give no slope: two or three points draw a line through any noise. */
  public static final int MIN_SAMPLES = 6;

  private Slope() {
  }

  /**
   * The least-squares slope of {@code values} over {@code hours}, in value units per hour; empty under {@value #MIN_SAMPLES}
   * samples or when every sample has the same hour.
   */
  public static OptionalDouble perHour(final double[] hours, final double[] values) {
    if (hours.length != values.length)
      throw new IllegalArgumentException("hours and values differ in length: " + hours.length + " vs " + values.length);
    final int n = hours.length;
    if (n < MIN_SAMPLES)
      return OptionalDouble.empty();
    double meanX = 0;
    double meanY = 0;
    for (int i = 0; i < n; i++) {
      meanX += hours[i];
      meanY += values[i];
    }
    meanX /= n;
    meanY /= n;
    double sxy = 0;
    double sxx = 0;
    for (int i = 0; i < n; i++) {
      final double dx = hours[i] - meanX;
      sxy += dx * (values[i] - meanY);
      sxx += dx * dx;
    }
    return sxx == 0 ? OptionalDouble.empty() : OptionalDouble.of(sxy / sxx);
  }
}
