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
package com.arcadedb.bench.workload.ramp;

import java.util.function.DoubleFunction;

/**
 * Finds the knee of an open-loop ramp: the highest arrival rate whose step still passed. Coarse steps multiply the rate by a
 * factor until one breaks, then bisection narrows the gap between the last passing and the first failing rate. With the coarse factor f, {@code n}
 * bisections locate the knee within about (f - 1) / 2^n of it (3.125% for 25% steps and 3 bisections).
 */
public final class RampSearch {
  private RampSearch() {
  }

  /**
   * @param ratePerS       the highest rate whose step passed, 0 when the first step already broke
   * @param p99Ms          the p99 of that passing step, 0 when there is none
   * @param steps          every step run, coarse and bisection
   * @param ceilingReached no step broke within the coarse budget, so the knee is only a lower bound
   * @param breakReason    why the last failing step broke; empty when none did
   */
  public record Knee(double ratePerS, double p99Ms, int steps, boolean ceilingReached, String breakReason) {
  }

  public static Knee search(final DoubleFunction<OpenLoop.StepResult> step, final double start, final double factor,
      final int maxCoarse, final int bisections) {
    if (!(start > 0) || Double.isInfinite(start))
      throw new IllegalArgumentException("start must be > 0: " + start);
    if (!(factor > 1) || Double.isInfinite(factor))
      throw new IllegalArgumentException("factor must be > 1: " + factor);
    if (maxCoarse < 1)
      throw new IllegalArgumentException("maxCoarse must be >= 1: " + maxCoarse);
    if (bisections < 0)
      throw new IllegalArgumentException("bisections must be >= 0: " + bisections);

    int steps = 0;
    double passRate = 0;
    double passP99 = 0;
    double failRate = 0;
    String reason = "";
    boolean broke = false;

    double rate = start;
    for (int i = 0; i < maxCoarse && !broke; i++) {
      final OpenLoop.StepResult result = step.apply(rate);
      steps++;
      if (result.broke()) {
        broke = true;
        failRate = rate;
        reason = result.why();
      } else {
        passRate = rate;
        passP99 = result.p99Ms();
        rate *= factor;
      }
    }

    if (!broke)
      return new Knee(passRate, passP99, steps, true, "");
    if (passRate == 0)
      return new Knee(0, 0, steps, false, reason);

    for (int i = 0; i < bisections; i++) {
      final double mid = (passRate + failRate) / 2;
      final OpenLoop.StepResult result = step.apply(mid);
      steps++;
      if (result.broke()) {
        failRate = mid;
        reason = result.why();
      } else {
        passRate = mid;
        passP99 = result.p99Ms();
      }
    }
    return new Knee(passRate, passP99, steps, false, reason);
  }
}
