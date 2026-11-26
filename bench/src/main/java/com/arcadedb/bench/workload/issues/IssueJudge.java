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
package com.arcadedb.bench.workload.issues;

/**
 * Pure verdicts for an issue scenario: a ratio of two forms, or an exponent fitted over several sizes. A scenario whose
 * slow form is cheaper than {@link #MIN_JUDGED_MS} is reported but not judged, because timer noise dominates there.
 */
public final class IssueJudge {
  public static final double MIN_JUDGED_MS = 5.0;

  public record Verdict(double value, boolean judged, boolean failed, String reason) {
  }

  private IssueJudge() {
  }

  public static Verdict ratio(final double slowMedianMs, final double referenceMedianMs, final double limit) {
    final double value = slowMedianMs / referenceMedianMs;
    if (Double.isNaN(value) || slowMedianMs < 0 || referenceMedianMs < 0)
      return new Verdict(value, true, true,
          "invalid medians: slow median " + slowMedianMs + " ms, reference median " + referenceMedianMs + " ms (limit " + limit + ")");
    final boolean judged = slowMedianMs >= MIN_JUDGED_MS;
    final boolean failed = judged && value > limit;
    final String reason = "slow median " + slowMedianMs + " ms / reference median " + referenceMedianMs + " ms = " + value + " (limit " + limit
        + (judged ? ")" : ", not judged: slow median under " + MIN_JUDGED_MS + " ms)");
    return new Verdict(value, judged, failed, reason);
  }

  /** Least-squares slope of ln(median) on ln(size). */
  public static double exponent(final double[] sizes, final double[] medians) {
    final int n = sizes.length;
    double sx = 0;
    double sy = 0;
    for (int i = 0; i < n; i++) {
      sx += Math.log(sizes[i]);
      sy += Math.log(medians[i]);
    }
    final double mx = sx / n;
    final double my = sy / n;
    double num = 0;
    double den = 0;
    for (int i = 0; i < n; i++) {
      final double dx = Math.log(sizes[i]) - mx;
      num += dx * (Math.log(medians[i]) - my);
      den += dx * dx;
    }
    return num / den;
  }

  public static Verdict exponent(final double[] sizes, final double[] medians, final double limit) {
    if (sizes.length < 2 || sizes.length != medians.length)
      return invalid("need at least 2 matching sizes and medians, got " + sizes.length + " sizes and " + medians.length + " medians");
    int largest = 0;
    boolean distinct = false;
    for (int i = 0; i < sizes.length; i++) {
      if (!(sizes[i] > 0) || !(medians[i] > 0))
        return invalid("size " + sizes[i] + " and median " + medians[i] + " at index " + i + " must both be positive");
      if (sizes[i] > sizes[largest])
        largest = i;
      if (sizes[i] != sizes[0])
        distinct = true;
    }
    if (!distinct)
      return invalid("all sizes are equal (" + sizes[0] + "), no exponent can be fitted");
    final double value = exponent(sizes, medians);
    final boolean judged = medians[largest] >= MIN_JUDGED_MS;
    final boolean failed = judged && value > limit;
    final String reason = "fitted exponent " + value + " (limit " + limit + ") over sizes of up to " + sizes[largest] + ", median at the largest "
        + medians[largest] + " ms" + (judged ? "" : ", not judged: under " + MIN_JUDGED_MS + " ms");
    return new Verdict(value, judged, failed, reason);
  }

  /**
   * The verdict of a second timing taken because {@code first} failed: it fails only when the second fails too, and then says
   * both. A regression of a fixed issue is many times its limit and fails twice; one noisy repetition does not.
   */
  public static Verdict confirm(final Verdict first, final Verdict second) {
    if (!first.failed() || !second.failed())
      return second;
    return new Verdict(second.value(), second.judged(), true, "failed twice: " + first.reason() + "; then " + second.reason());
  }

  private static Verdict invalid(final String why) {
    return new Verdict(Double.NaN, true, true, "invalid exponent input: " + why);
  }
}
