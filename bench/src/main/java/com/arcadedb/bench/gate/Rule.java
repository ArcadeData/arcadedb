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
package com.arcadedb.bench.gate;

/** The regression rule: a relative band widened to the measured spread, plus an absolute floor per unit. */
final class Rule {

  static final double MIN_BAND = 0.10;

  private static final double MS_FLOOR       = 1.0;
  private static final double S_FLOOR        = 0.5;
  private static final double BYTE_FLOOR     = 1048576;
  /** An issue ratio (unit {@code factor}) must move by a whole factor to be news: a ratio near 1 wobbles night to night. */
  private static final double FACTOR_FLOOR   = 1.0;
  /** An issue exponent (unit {@code exponent}) must move by a quarter to be news: a fit over three sizes wobbles too. */
  private static final double EXPONENT_FLOOR = 0.25;

  private Rule() {
  }

  static double absoluteFloor(final String unit) {
    if (unit == null)
      return 0;
    return switch (unit) {
      case "ms" -> MS_FLOOR;
      case "s" -> S_FLOOR;
      case "bytes" -> BYTE_FLOOR;
      case "factor" -> FACTOR_FLOOR;
      case "exponent" -> EXPONENT_FLOOR;
      default -> 0;
    };
  }

  /** (max - min) / median of the repetition values; 0 with fewer than 2 values or a zero median. */
  static double spread(final double median, final double[] values) {
    if (values == null || values.length < 2 || median == 0)
      return 0;
    double min = values[0];
    double max = values[0];
    for (final double v : values) {
      if (v < min)
        min = v;
      if (v > max)
        max = v;
    }
    return (max - min) / Math.abs(median);
  }

  static double band(final Point baseline, final Point newest) {
    return Math.max(Math.max(spread(baseline.median(), baseline.values()), spread(newest.median(), newest.values())), MIN_BAND);
  }

  /** Relative change, positive = worse; 0 when the base is 0. */
  static double change(final Point baseline, final Point newest) {
    if (baseline.median() == 0)
      return 0;
    return signedDiff(baseline, newest) / Math.abs(baseline.median());
  }

  /** Compares a metric pair, or a status pair (NaN medians). */
  static Verdict.Kind compare(final Point baseline, final Point newest) {
    if (isStatus(baseline) || isStatus(newest)) {
      if (baseline.valid() && !newest.valid())
        return Verdict.Kind.WORSE;
      if (!baseline.valid() && newest.valid())
        return Verdict.Kind.BETTER;
      return Verdict.Kind.SAME;
    }
    final double diff = signedDiff(baseline, newest);
    if (Math.abs(newest.median() - baseline.median()) < absoluteFloor(newest.unit()))
      return Verdict.Kind.SAME;
    final boolean zeroBase = baseline.median() == 0;
    final double change = change(baseline, newest);
    final double band = band(baseline, newest);
    if (zeroBase ? diff > 0 : change > band)
      return Verdict.Kind.WORSE;
    if (zeroBase ? diff < 0 : change < -band)
      return Verdict.Kind.BETTER;
    return Verdict.Kind.SAME;
  }

  private static boolean isStatus(final Point p) {
    return Double.isNaN(p.median());
  }

  /** Positive = worse. */
  private static double signedDiff(final Point baseline, final Point newest) {
    return "higher".equals(newest.better()) ? baseline.median() - newest.median() : newest.median() - baseline.median();
  }
}
