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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RampSearchTest {
  private static final double CAPACITY = 7_300;

  /** Passes (p99 5 ms) up to the capacity, breaks (p99 100 ms) above it. */
  private static DoubleFunction<OpenLoop.StepResult> capacity(final double capacity, final AtomicInteger calls) {
    return rate -> {
      calls.incrementAndGet();
      return rate <= capacity ? new OpenLoop.StepResult(rate, 1000, 0, 0, 0, 5.0 + rate / 10_000, 2.0)
          : new OpenLoop.StepResult(rate, 1000, 0, 0, 0, 100.0, 40.0);
    };
  }

  @Test
  void kneeWithinThreePercentOfCapacity() {
    // a capacity just below a coarse step (1000 * 1.25^9 = 7450.6) is the near-worst case for 3 bisections (3.125%)
    final double capacity = 7_450;
    final RampSearch.Knee knee = RampSearch.search(capacity(capacity, new AtomicInteger()), 1_000, 1.25, 30, 3);
    assertThat(knee.ratePerS()).isBetween(0.968 * capacity, capacity);
    assertThat(knee.ceilingReached()).isFalse();
    assertThat(knee.p99Ms()).isEqualTo(5.0 + knee.ratePerS() / 10_000);
    assertThat(knee.breakReason()).contains("p99");
  }

  @Test
  void coarseThenBisectionStepCount() {
    final AtomicInteger calls = new AtomicInteger();
    final RampSearch.Knee knee = RampSearch.search(capacity(CAPACITY, calls), 1_000, 1.25, 30, 4);
    // 1000 * 1.25^n: nine passing steps (last 5960), then 7450 breaks = 10 coarse, plus 4 bisections
    assertThat(knee.steps()).isEqualTo(14);
    assertThat(calls.get()).isEqualTo(14);
  }

  @Test
  void breaksAtFirstStepGivesZero() {
    final RampSearch.Knee knee = RampSearch.search(capacity(500, new AtomicInteger()), 1_000, 1.25, 30, 4);
    assertThat(knee.ratePerS()).isZero();
    assertThat(knee.p99Ms()).isZero();
    assertThat(knee.steps()).isEqualTo(1);
    assertThat(knee.ceilingReached()).isFalse();
    assertThat(knee.breakReason()).contains("p99 100.0 ms");
  }

  @Test
  void neverBreaksReportsTheCeiling() {
    final AtomicInteger calls = new AtomicInteger();
    final RampSearch.Knee knee = RampSearch.search(capacity(Double.MAX_VALUE, calls), 1_000, 2, 3, 4);
    assertThat(knee.ceilingReached()).isTrue();
    assertThat(knee.ratePerS()).isEqualTo(4_000);
    assertThat(knee.steps()).isEqualTo(3);
    assertThat(knee.breakReason()).isEmpty();
  }

  @Test
  void kneeP99ComesFromThePassingStepNotABrokenOne() {
    // every arrival above the capacity fails: p99 reads 0 but the step broke
    final DoubleFunction<OpenLoop.StepResult> step = rate -> rate <= CAPACITY ? new OpenLoop.StepResult(rate, 1000, 0, 0, 0, 7.0, 2.0)
        : new OpenLoop.StepResult(rate, 1000, 1000, 0, 0, 0.0, 0.0);
    final RampSearch.Knee knee = RampSearch.search(step, 1_000, 1.25, 30, 4);
    assertThat(knee.p99Ms()).isEqualTo(7.0);
    assertThat(knee.breakReason()).contains("errors");
  }

  @Test
  void breakReasonComesFromTheBisectionStepThatBoundedTheKnee() {
    final double[] lastFailing = { 0 };
    final DoubleFunction<OpenLoop.StepResult> step = rate -> {
      if (rate <= CAPACITY)
        return new OpenLoop.StepResult(rate, 1_000_000, 0, 0, 0, 5.0, 2.0);
      lastFailing[0] = rate;
      return new OpenLoop.StepResult(rate, 1_000_000, (long) rate * 10, 0, 0, 5.0, 2.0);
    };
    final RampSearch.Knee knee = RampSearch.search(step, 1_000, 1.25, 30, 4);
    assertThat(lastFailing[0]).isLessThan(1_000 * Math.pow(1.25, 9));
    assertThat(knee.breakReason()).contains("errors " + ((long) lastFailing[0] * 10) + " of");
  }

  @Test
  void flakyStepFunctionStillYieldsAPassingRateBelowTheFailingOne() {
    final double[] lastFailing = { 0 };
    final DoubleFunction<OpenLoop.StepResult> step = rate -> {
      if (rate <= 5_961 || rate > 6_800 && rate <= 7_300)
        return new OpenLoop.StepResult(rate, 1000, 0, 0, 0, 5.0, 2.0);
      lastFailing[0] = rate;
      return new OpenLoop.StepResult(rate, 1000, 0, 0, 0, 100.0, 40.0);
    };
    final RampSearch.Knee knee = RampSearch.search(step, 1_000, 1.25, 30, 4);
    assertThat(step.apply(knee.ratePerS()).broke()).isFalse();
    assertThat(knee.ratePerS()).isLessThan(lastFailing[0]);
    assertThat(knee.ratePerS()).isLessThanOrEqualTo(7_300);
  }

  @Test
  void rejectsNonFiniteArguments() {
    final DoubleFunction<OpenLoop.StepResult> step = capacity(CAPACITY, new AtomicInteger());
    assertThatThrownBy(() -> RampSearch.search(step, Double.NaN, 1.25, 3, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RampSearch.search(step, Double.POSITIVE_INFINITY, 1.25, 3, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RampSearch.search(step, 1, Double.NaN, 3, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RampSearch.search(step, 1, Double.POSITIVE_INFINITY, 3, 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsInvalidArguments() {
    final DoubleFunction<OpenLoop.StepResult> step = capacity(CAPACITY, new AtomicInteger());
    assertThatThrownBy(() -> RampSearch.search(step, 0, 1.25, 3, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RampSearch.search(step, 1, 1.0, 3, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RampSearch.search(step, 1, 1.25, 0, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RampSearch.search(step, 1, 1.25, 3, -1)).isInstanceOf(IllegalArgumentException.class);
  }
}
