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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.BenchRunner;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.workload.ramp.OpenLoop;
import com.arcadedb.bench.workload.ramp.RampSearch;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class RampWorkloadTest {
  @Test
  void casesAndDeclaredRepetitions() {
    final RampWorkload workload = new RampWorkload();
    assertThat(workload.name()).isEqualTo("ramp");
    assertThat(workload.cases()).containsExactly("knee-point-lookup-cypher");
    // one ramp takes about 5 min at S: the weeknight S lane has no room for it, so it runs from M up (every Saturday)
    assertThat(workload.cases(Scale.S)).isEmpty();
    for (final Scale scale : new Scale[] { Scale.XS, Scale.M, Scale.L })
      assertThat(workload.cases(scale)).isEqualTo(workload.cases());
    assertThat(workload.reps()).hasValue(3);
    assertThat(workload.warmup()).hasValue(1);
    assertThat(WorkloadRegistry.all()).hasAtLeastOneElementOfType(RampWorkload.class);
  }

  @Test
  void registryListsMixedAndHybridRag() {
    assertThat(WorkloadRegistry.all()).extracting(Workload::name).contains("mixed", "hybrid-rag");
  }

  @Test
  void stepLengthsAreTheSpecs() {
    final RampWorkload workload = new RampWorkload();
    assertThat(workload.stepLength(Scale.XS)).isEqualTo(Duration.ofSeconds(10));
    assertThat(workload.stepLength(Scale.S)).isEqualTo(Duration.ofSeconds(10));
    assertThat(workload.stepLength(Scale.M)).isEqualTo(Duration.ofSeconds(20));
    assertThat(workload.stepLength(Scale.L)).isEqualTo(Duration.ofSeconds(20));
    assertThat(RampWorkload.WARM).isEqualTo(Duration.ofSeconds(2));
    assertThat(RampWorkload.START_PER_S).isEqualTo(100.0);
    assertThat(RampWorkload.FACTOR).isEqualTo(1.25);
    assertThat(RampWorkload.MAX_COARSE).isEqualTo(40);
    assertThat(RampWorkload.BISECTIONS).isEqualTo(3);
    assertThat(RampWorkload.CLIENTS).isEqualTo(128);
    assertThat(RampWorkload.LATE_LIMIT).isEqualTo(0.01);
    // the IT's override shortens the steps, never the spec's
    assertThat(new RampWorkload(Duration.ofSeconds(1), Duration.ofMillis(500), 8, 0.05).stepLength(Scale.M)).isEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void metricsAreTheSpecs() throws Exception {
    final RampSearch.Knee knee = new RampSearch.Knee(5_000, 12.5, 21, false, "p99 80.0 ms > 50 ms");
    final OpenLoop.StepResult kneeStep = new OpenLoop.StepResult(5_000, 50_000, 0, 0, 10, 12.5, 3.0);
    final OpenLoop.StepResult breakStep = new OpenLoop.StepResult(5_200, 52_000, 0, 0, 0, 80.0, 3.0);
    final Map<String, Metric> metrics = byName(RampWorkload.publish(knee, kneeStep, breakStep).perRep());
    assertThat(metrics.keySet()).containsExactly("knee_per_s", "knee_p99_ms", "steps", "client_limited", "ceiling_reached",
        "break_drop", "break_p99", "break_errors", "break_unfinished");
    assertMetric(metrics.get("knee_per_s"), "ops/s", Metric.Better.HIGHER, 5_000);
    assertMetric(metrics.get("knee_p99_ms"), "ms", Metric.Better.LOWER, 12.5);
    // informational: units the gate never judges (Rule.absoluteFloor is infinite for them)
    assertMetric(metrics.get("steps"), "steps", Metric.Better.LOWER, 21);
    assertMetric(metrics.get("client_limited"), "flag", Metric.Better.LOWER, 0);
    assertMetric(metrics.get("ceiling_reached"), "flag", Metric.Better.LOWER, 0);
    assertMetric(metrics.get("break_drop"), "flag", Metric.Better.LOWER, 0);
    assertMetric(metrics.get("break_p99"), "flag", Metric.Better.LOWER, 1);
    assertMetric(metrics.get("break_errors"), "flag", Metric.Better.LOWER, 0);
    assertMetric(metrics.get("break_unfinished"), "flag", Metric.Better.LOWER, 0);
  }

  @Test
  void aCeilingIsReportedNotFaked() throws Exception {
    final RampSearch.Knee knee = new RampSearch.Knee(600_000, 4.0, 40, true, "");
    final OpenLoop.StepResult kneeStep = new OpenLoop.StepResult(600_000, 6_000_000, 0, 0, 0, 4.0, 1.0);
    final Map<String, Metric> metrics = byName(RampWorkload.publish(knee, kneeStep, null).perRep());
    assertThat(metrics.get("knee_per_s").median()).isEqualTo(600_000);
    assertThat(metrics.get("ceiling_reached").median()).isEqualTo(1);
    // nothing broke the ramp
    for (final String flag : List.of("break_drop", "break_p99", "break_errors", "break_unfinished"))
      assertThat(metrics.get(flag).median()).as(flag).isZero();
  }

  @Test
  void aClientLimitedKneeFailsTheCaseWithItsMetrics() {
    final RampSearch.Knee knee = new RampSearch.Knee(5_000, 12.5, 21, false, "dropped 3 arrivals (no free client)");
    // 1% of 50,000 arrivals is 500: 501 late starts is over it
    final OpenLoop.StepResult kneeStep = new OpenLoop.StepResult(5_000, 50_000, 0, 0, 501, 12.5, 3.0);
    final OpenLoop.StepResult breakStep = new OpenLoop.StepResult(6_250, 62_500, 0, 3, 0, 10.0, 3.0);
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> RampWorkload.publish(knee, kneeStep, breakStep));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("client").contains("501 of 50000");
    final Map<String, Metric> metrics = byName(e.metrics());
    assertThat(metrics.get("client_limited").median()).isEqualTo(1);
    assertThat(metrics.get("knee_per_s").median()).isEqualTo(5_000);

    // exactly 1% is not over it
    final OpenLoop.StepResult atLimit = new OpenLoop.StepResult(5_000, 50_000, 0, 0, 500, 12.5, 3.0);
    assertThat(catchThrowableOfType(CaseFailedWithMetrics.class, () -> RampWorkload.publish(knee, atLimit, breakStep))).isNull();
  }

  @Test
  void aKneeOfZeroFailsTheCase() {
    final RampSearch.Knee knee = new RampSearch.Knee(0, 0, 1, false, "p99 120.0 ms > 50 ms");
    final OpenLoop.StepResult first = new OpenLoop.StepResult(100, 1_000, 0, 0, 0, 120.0, 30.0);
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class, () -> RampWorkload.publish(knee, null, first));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("first step").contains("p99 120.0 ms > 50 ms");
    assertThat(byName(e.metrics()).get("knee_per_s").median()).isZero();
    assertThat(byName(e.metrics()).get("break_p99").median()).isEqualTo(1);
  }

  @Test
  void theKneeStepIsTheLastPassingStep() throws Exception {
    // passes up to 1,000/s: coarse 931/s passes and 1,164/s breaks, the bisections run 1,048 (breaks), 990 (passes) and
    // 1,019 (breaks), so the knee step is the 990/s bisection, not the last step run nor the last coarse pass
    final List<Double> rates = new ArrayList<>();
    final DoubleFunction<OpenLoop.StepResult> step = rate -> {
      rates.add(rate);
      if (rate > 1_000)
        return new OpenLoop.StepResult(rate, 1_000, 0, 0, 0, 90.0, 40.0);
      // late starts only on the knee's neighbourhood: the last passing step has 2% of them
      final long late = rate > 950 ? 200 : 0;
      return new OpenLoop.StepResult(rate, 10_000, 0, 0, late, rate / 100, 1.0);
    };
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> RampWorkload.ramp(step, 100, 1.25, 40, 3));
    assertThat(e).isNotNull();
    final Map<String, Metric> metrics = byName(e.metrics());
    final double knee = metrics.get("knee_per_s").median();
    assertThat(knee).isGreaterThan(950).isLessThanOrEqualTo(1_000);
    assertThat(metrics.get("knee_p99_ms").median()).isEqualTo(knee / 100);
    assertThat(metrics.get("steps").median()).isEqualTo(rates.size());
    assertThat(metrics.get("client_limited").median()).isEqualTo(1);

    // the same ramp with no late starts passes
    final Measurement ok = RampWorkload.ramp(rate -> rate > 1_000 ? new OpenLoop.StepResult(rate, 1_000, 0, 0, 0, 90.0, 40.0)
        : new OpenLoop.StepResult(rate, 10_000, 0, 0, 0, rate / 100, 1.0), 100, 1.25, 40, 3);
    assertThat(byName(ok.perRep()).get("client_limited").median()).isZero();
  }

  @Test
  void aStepBlockedByAnEarlierUnfinishedRequestFailsTheCaseWithTheRampsMetrics() {
    // coarse 931/s passes, 1,164/s breaks with an unfinished request, and the first bisection step cannot start because that
    // request still holds its client: the case fails, but with the knee so far and the unfinished break, not with a bare
    // exception that loses every number
    final List<Double> rates = new ArrayList<>();
    final DoubleFunction<OpenLoop.StepResult> step = rate -> {
      rates.add(rate);
      if (rate > 1_100)
        return new OpenLoop.StepResult(rate, 11_640, 0, 0, 0, 5.0, 1.0, 1);
      if (rate > 1_000)
        throw new OpenLoop.ClientsStillHeldException(1);
      return new OpenLoop.StepResult(rate, 10_000, 0, 0, 0, rate / 100, 1.0);
    };
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> RampWorkload.ramp(step, 100, 1.25, 40, 3));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("1 requests of an earlier step");
    final Map<String, Metric> metrics = byName(e.metrics());
    final double knee = metrics.get("knee_per_s").median();
    assertThat(knee).isGreaterThan(900).isLessThan(1_000);
    assertThat(metrics.get("knee_p99_ms").median()).isEqualTo(knee / 100);
    assertThat(metrics.get("steps").median()).as("the blocked step never ran").isEqualTo(rates.size() - 1);
    assertThat(metrics.get("break_unfinished").median()).isEqualTo(1);
    assertThat(metrics.get("ceiling_reached").median()).isZero();
  }

  @Test
  void breakFlagsMapEachReasonOfAStep() {
    // ratePerS, arrivals, errors, dropped, lateStarts, p99Ms, p50Ms, unfinished
    assertThat(RampWorkload.breakFlags(null)).containsExactly(0, 0, 0, 0);
    assertThat(RampWorkload.breakFlags(new OpenLoop.StepResult(1_000, 10_000, 10, 0, 0, 50.0, 1.0, 0)))
        .as("at every limit, nothing broke").containsExactly(0, 0, 0, 0);
    assertThat(RampWorkload.breakFlags(new OpenLoop.StepResult(1_000, 10_000, 0, 1, 0, 1.0, 1.0, 0))).containsExactly(1, 0, 0, 0);
    assertThat(RampWorkload.breakFlags(new OpenLoop.StepResult(1_000, 10_000, 0, 0, 0, 50.1, 1.0, 0))).containsExactly(0, 1, 0, 0);
    assertThat(RampWorkload.breakFlags(new OpenLoop.StepResult(1_000, 10_000, 11, 0, 0, 1.0, 1.0, 0))).containsExactly(0, 0, 1, 0);
    assertThat(RampWorkload.breakFlags(new OpenLoop.StepResult(1_000, 10_000, 0, 0, 0, 1.0, 1.0, 2))).containsExactly(0, 0, 0, 1);
    // a step broken by several reasons raises each of them
    assertThat(RampWorkload.breakFlags(new OpenLoop.StepResult(1_000, 10_000, 50, 4, 0, 90.0, 1.0, 0))).containsExactly(1, 1, 1, 0);
  }

  @Test
  void theBreakFlagsAreTheLastBrokenStepsNotTheFirst() throws Exception {
    // coarse 1,164/s breaks on p99; the bisections 1,048 and 1,019 break on drops: the knee was bounded by the last, a drop
    final Measurement m = RampWorkload.ramp(rate -> {
      if (rate > 1_100)
        return new OpenLoop.StepResult(rate, 1_000, 0, 0, 0, 90.0, 40.0);
      if (rate > 1_000)
        return new OpenLoop.StepResult(rate, 1_000, 0, 2, 0, 5.0, 1.0);
      return new OpenLoop.StepResult(rate, 10_000, 0, 0, 0, 5.0, 1.0);
    }, 100, 1.25, 40, 3);
    final Map<String, Metric> metrics = byName(m.perRep());
    assertThat(metrics.get("break_drop").median()).isEqualTo(1);
    assertThat(metrics.get("break_p99").median()).isZero();
  }

  @Test
  void aFailedWarmUpRampIsNotTheCasesVerdict() {
    // the first ramp of the case (the warm-up) is client limited, the three measured ones are clean: the case is OK and its
    // numbers are the measured ramps', not the warm-up's
    final AtomicInteger ramps = new AtomicInteger();
    final RampWorkload workload = new RampWorkload() {
      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) {
        startCase();
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws CaseFailedWithMetrics {
        final long late = ramps.getAndIncrement() == 0 ? 500 : 0;
        return counted(() -> RampWorkload.ramp(rate -> rate > 1_000 ? new OpenLoop.StepResult(rate, 1_000, 0, 1, 0, 5.0, 1.0)
            : new OpenLoop.StepResult(rate, 10_000, 0, 0, late, 5.0, 1.0), 100, 1.25, 40, 3));
      }
    };
    final CaseResult r = new BenchRunner(5, 2, Duration.ofSeconds(10)).run(workload,
        new WorkloadContext(null, Scale.XS, 1L, new JSONObject()), n -> true).get(0);
    assertThat(r.status()).as(r.error()).isEqualTo(CaseResult.Status.OK);
    assertThat(ramps.get()).isEqualTo(4);
    assertThat(byName(r.metrics()).get("client_limited").values()).containsExactly(0, 0, 0);

    // a measured ramp that is client limited still fails the case
    ramps.set(0);
    final RampWorkload lateRep = new RampWorkload() {
      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) {
        startCase();
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws CaseFailedWithMetrics {
        final long late = ramps.getAndIncrement() == 2 ? 500 : 0;
        return counted(() -> RampWorkload.ramp(rate -> rate > 1_000 ? new OpenLoop.StepResult(rate, 1_000, 0, 1, 0, 5.0, 1.0)
            : new OpenLoop.StepResult(rate, 10_000, 0, 0, late, 5.0, 1.0), 100, 1.25, 40, 3));
      }
    };
    final CaseResult failed = new BenchRunner(5, 2, Duration.ofSeconds(10)).run(lateRep,
        new WorkloadContext(null, Scale.XS, 1L, new JSONObject()), n -> true).get(0);
    assertThat(failed.status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(failed.error()).contains("client limited");
  }

  private static Map<String, Metric> byName(final List<Metric> metrics) {
    final Map<String, Metric> map = new LinkedHashMap<>();
    for (final Metric m : metrics)
      map.put(m.name(), m);
    return map;
  }

  private static void assertMetric(final Metric m, final String unit, final Metric.Better better, final double value) {
    assertThat(m).isNotNull();
    assertThat(m.unit()).as(m.name()).isEqualTo(unit);
    assertThat(m.better()).as(m.name()).isEqualTo(better);
    assertThat(m.median()).as(m.name()).isEqualTo(value);
  }
}
