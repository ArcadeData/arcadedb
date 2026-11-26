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

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.workload.issues.ExponentScenario;
import com.arcadedb.bench.workload.issues.IssueJudge;
import com.arcadedb.bench.workload.issues.IssueSampler;
import com.arcadedb.bench.workload.issues.IssueScenario;
import com.arcadedb.bench.workload.issues.RatioScenario;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static com.arcadedb.bench.workload.issues.IssueScenario.Statement.sql;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.offset;

class IssuesWorkloadTest {
  @Test
  void casesAreTheTenOfThePlan() {
    final IssuesWorkload workload = new IssuesWorkload();
    assertThat(workload.name()).isEqualTo("issues");
    assertThat(workload.cases()).containsExactly("issue-12725", "issue-12713", "issue-12281", "issue-11963", "issue-12522",
        "issue-12141", "issue-11701", "issue-11871", "issue-11682", "issue-11700");
    for (final Scale scale : Scale.values())
      assertThat(workload.cases(scale)).isEqualTo(workload.cases());
    assertThat(WorkloadRegistry.all().getLast()).isInstanceOf(IssuesWorkload.class);
  }

  @Test
  void everyLimitIsTheTables() {
    final Map<String, Double> table = new LinkedHashMap<>();
    table.put("issue-12725", 3.0);
    table.put("issue-12713", 2.5);
    table.put("issue-12281", 10.0);
    table.put("issue-11963", 10.0);
    table.put("issue-12522", 20.0);
    table.put("issue-12141", 4.0);
    table.put("issue-11701", 5.0);
    table.put("issue-11871", 5.0);
    table.put("issue-11682", 1.3);
    table.put("issue-11700", 0.55);
    final Map<String, Double> actual = new LinkedHashMap<>();
    for (final IssueScenario s : IssuesWorkload.scenarios()) {
      actual.put(s.caseName(), s.limit());
      assertThat(s.caseName()).isEqualTo("issue-" + s.issue());
    }
    assertThat(actual).containsExactlyEntriesOf(table);
    // the two sweeps are exponents, every other case a ratio
    for (final IssueScenario s : IssuesWorkload.scenarios())
      assertThat(s instanceof ExponentScenario).as(s.caseName()).isEqualTo(s.issue() == 11682 || s.issue() == 11700);
  }

  @Test
  void formsMustAgree() {
    final RatioScenario differ = fake(UNORDERED, List.of("a", "b", "c"), List.of("a", "b"));
    assertThatThrownBy(() -> differ.checkForms(null)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("3 rows").hasMessageContaining("2 rows");

    final RatioScenario sameCountOtherRows = fake(UNORDERED, List.of("a", "b"), List.of("a", "x"));
    assertThatThrownBy(() -> sameCountOtherRows.checkForms(null)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("differ");

    final RatioScenario empty = fake(UNORDERED, List.of(), List.of());
    assertThatThrownBy(() -> empty.checkForms(null)).isInstanceOf(IllegalStateException.class).hasMessageContaining("no row");

    fake(UNORDERED, List.of("a", "b"), List.of("a", "b")).checkForms(null);
  }

  @Test
  void unorderedFormsAgreeInAnyOrder() {
    // a slow form that falls back to a scan answers in bucket order: it must be judged by its factor, not fail the check
    fake(UNORDERED, List.of("c", "a", "b", "a"), List.of("a", "a", "b", "c")).checkForms(null);
    // a multiset, not a set: a duplicate lost is a difference
    assertThatThrownBy(() -> fake(UNORDERED, List.of("a", "b", "b"), List.of("a", "a", "b")).checkForms(null))
        .hasMessageContaining("differ");
  }

  @Test
  void orderedFormsMustAgreeInOrder() {
    assertThatThrownBy(() -> fake(ORDERED, List.of("b", "a"), List.of("a", "b")).checkForms(null))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("differ");
    fake(ORDERED, List.of("a", "b"), List.of("a", "b")).checkForms(null);
  }

  @Test
  void aFailedVerdictKeepsItsMetrics() {
    final IssueJudge.Verdict failed = IssueJudge.ratio(60.0, 2.0, 3.0);
    assertThat(failed.failed()).isTrue();
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> RatioScenario.publish(failed, 60.0, 2.0, 5.5));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).isEqualTo(failed.reason());
    assertThat(e.metrics()).extracting(Metric::name).containsExactly("factor", "slow_ms", "reference_ms", "rep_s");
    assertThat(e.metrics().getFirst().unit()).isEqualTo("factor");
    assertThat(e.metrics().getFirst().better()).isEqualTo(Metric.Better.LOWER);
    assertThat(e.metrics().getFirst().median()).isEqualTo(30.0);
    assertThat(e.metrics().get(1).unit()).isEqualTo("ms");

    final IssueJudge.Verdict failedExponent = IssueJudge.exponent(new double[] { 1, 2, 4 }, new double[] { 10, 40, 160 }, 1.3);
    final CaseFailedWithMetrics ex = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> ExponentScenario.publish(failedExponent, new double[] { 10, 40, 160 }, 5.5));
    assertThat(ex.metrics()).extracting(Metric::name)
        .containsExactly("exponent", "size_1_ms", "size_2_ms", "size_3_ms", "rep_s");
    assertThat(ex.metrics().get(1).median()).isEqualTo(10.0);
    assertThat(ex.metrics().get(3).median()).isEqualTo(160.0);
    assertThat(ex.metrics().getFirst().unit()).isEqualTo("exponent");
    assertThat(ex.metrics().getFirst().better()).isEqualTo(Metric.Better.LOWER);
    assertThat(ex.metrics().getFirst().median()).isCloseTo(2.0, offset(0.01));
  }

  @Test
  void aPassingVerdictReturnsItsMetrics() throws Exception {
    final Measurement m = RatioScenario.publish(IssueJudge.ratio(6.0, 4.0, 3.0), 6.0, 4.0, 5.5);
    assertThat(m.perRep()).extracting(Metric::name).containsExactly("factor", "slow_ms", "reference_ms", "rep_s");
    assertThat(m.perRep().getFirst().median()).isEqualTo(1.5);
    // not judged: a slow form under 5 ms passes whatever the ratio
    final Measurement fast = RatioScenario.publish(IssueJudge.ratio(1.0, 0.01, 3.0), 1.0, 0.01, 5.5);
    assertThat(fast.perRep().getFirst().median()).isEqualTo(100.0);
  }

  @Test
  void aRatioRepetitionAtSLastsAtLeastFiveSecondsAfterAColdCalibration() throws Exception {
    // the first 100 executions are cold (5 ms each), then the forms cost 0.2 and 0.1 ms: the calibration overestimates the
    // cost 30 times, and the repetition must still last 5 s of the sampler's clock
    final AtomicLong clock = new AtomicLong();
    final int[] executions = { 0 };
    final RatioScenario scenario = new RatioScenario(1, 3.0, List.of(sql("SLOW")), List.of(sql("REFERENCE")),
        (scale, seed) -> List.of(Map.of()), (db, scale, seed) -> {
    }) {
      @Override
      protected void execute(final RemoteDatabase db, final IssueScenario.Statement statement, final Map<String, Object> params) {
        final boolean cold = executions[0]++ < 100;
        clock.addAndGet(cold ? 5_000_000L : "SLOW".equals(statement.text()) ? 200_000L : 100_000L);
      }
    };
    scenario.bind(Scale.S, 1L);
    final Measurement m = scenario.measure(null, new IssueSampler(clock::get), Scale.S);
    assertThat(metric(m, "rep_s")).isGreaterThanOrEqualTo(IssueScenario.SAMPLES * IssueScenario.sampleTargetMs(Scale.S) / 1_000);
    assertThat(metric(m, "rep_s")).isGreaterThanOrEqualTo(5.0);
    assertThat(metric(m, "slow_ms")).isCloseTo(0.2, offset(1e-6));
    assertThat(metric(m, "reference_ms")).isCloseTo(0.1, offset(1e-6));
    assertThat(metric(m, "factor")).isCloseTo(2.0, offset(1e-6));
  }

  @Test
  void anExponentRepetitionAtSLastsAtLeastFiveSecondsAndShowsEverySize() throws Exception {
    final AtomicLong clock = new AtomicLong();
    final ExponentScenario scenario = new ExponentScenario(2, 1.3, (scale, seed) -> {
      final List<ExponentScenario.Point> points = new ArrayList<>();
      for (final int size : new int[] { 100, 200, 400 })
        points.add(new ExponentScenario.Point(size, sql("Q"), Map.of("size", size), 1, false));
      return points;
    }, (db, scale, seed) -> {
    }) {
      @Override
      protected void execute(final RemoteDatabase db, final IssueScenario.Statement statement, final Map<String, Object> params) {
        // linear: 0.01 ms per unit of size
        clock.addAndGet(10_000L * (Integer) params.get("size"));
      }
    };
    scenario.bind(Scale.S, 1L);
    final Measurement m = scenario.measure(null, new IssueSampler(clock::get), Scale.S);
    assertThat(metric(m, "rep_s")).isGreaterThanOrEqualTo(5.0);
    assertThat(metric(m, "exponent")).isCloseTo(1.0, offset(1e-6));
    assertThat(metric(m, "size_1_ms")).isCloseTo(1.0, offset(1e-6));
    assertThat(metric(m, "size_2_ms")).isCloseTo(2.0, offset(1e-6));
    assertThat(metric(m, "size_3_ms")).isCloseTo(4.0, offset(1e-6));
  }

  @Test
  void aFailedRepetitionIsTimedAgainAndPassesWhenTheSecondTimingDoes() throws Exception {
    // the slow form costs 60 ms until 6 s of the fake clock (calibration, warm-up and the first timing), then 2 ms like the
    // reference: the first verdict fails (30x, limit 3), the second passes, and one noisy repetition does not fail the case
    final AtomicLong clock = new AtomicLong();
    final RatioScenario scenario = clocked(clock, () -> clock.get() < 6_000_000_000L ? 60_000_000L : 2_000_000L);
    final Measurement m = scenario.measure(null, new IssueSampler(clock::get), Scale.S);
    assertThat(metric(m, "factor")).isCloseTo(1.0, offset(1e-6));
    assertThat(clock.get()).as("two timings of 5 s, after calibration and warm-up").isGreaterThan(11_000_000_000L);
  }

  @Test
  void aRegressionFailsBothTimingsAndThrowsWithItsMetrics() {
    final AtomicLong clock = new AtomicLong();
    final RatioScenario scenario = clocked(clock, () -> 60_000_000L);
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> scenario.measure(null, new IssueSampler(clock::get), Scale.S));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).startsWith("failed twice: ").contains("limit 3.0");
    assertThat(e.metrics()).extracting(Metric::name).containsExactly("factor", "slow_ms", "reference_ms", "rep_s");
    assertThat(e.metrics().getFirst().median()).isCloseTo(30.0, offset(1e-6));
    assertThat(e.metrics().get(3).median()).isGreaterThanOrEqualTo(5.0);
  }

  @Test
  void aQuadraticSweepFailsBothTimingsAndThrowsWithItsMetrics() {
    final AtomicLong clock = new AtomicLong();
    final ExponentScenario scenario = new ExponentScenario(2, 1.3, (scale, seed) -> {
      final List<ExponentScenario.Point> points = new ArrayList<>();
      for (final int size : new int[] { 100, 200, 400 })
        points.add(new ExponentScenario.Point(size, sql("Q"), Map.of("size", size), 1, false));
      return points;
    }, (db, scale, seed) -> {
    }) {
      @Override
      protected void execute(final RemoteDatabase db, final IssueScenario.Statement statement, final Map<String, Object> params) {
        final long size = (Integer) params.get("size");
        clock.addAndGet(100L * size * size);
      }
    };
    scenario.bind(Scale.S, 1L);
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> scenario.measure(null, new IssueSampler(clock::get), Scale.S));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).startsWith("failed twice: ");
    assertThat(e.metrics().getFirst().median()).isCloseTo(2.0, offset(1e-6));
  }

  @Test
  void aReferenceCountingZeroNamesTheLikelyCause() {
    final RatioScenario scenario = new RatioScenario(1, 3.0, UNORDERED, UNORDERED, (scale, seed) -> List.of(Map.of()), (db, scale, seed) -> {
    }) {
      @Override
      protected List<String> rows(final RemoteDatabase db, final boolean slow, final int p, final int s) {
        return List.of(slow ? "{\"n\":1169}" : "{\"n\":0}");
      }
    }.hintWhenReferenceCountsZero("the server likely predates #12187");
    scenario.bind(Scale.XS, 1L);
    assertThatThrownBy(() -> scenario.checkForms(null)).hasMessageContaining("differ").hasMessageContaining("predates #12187");
  }

  /** A ratio scenario on a fake clock: the slow form costs what {@code slowNanos} says at the time, the reference 2 ms. */
  private static RatioScenario clocked(final AtomicLong clock, final LongSupplier slowNanos) {
    final RatioScenario scenario = new RatioScenario(1, 3.0, List.of(sql("SLOW")), List.of(sql("REFERENCE")),
        (scale, seed) -> List.of(Map.of()), (db, scale, seed) -> {
    }) {
      @Override
      protected void execute(final RemoteDatabase db, final IssueScenario.Statement statement, final Map<String, Object> params) {
        clock.addAndGet("SLOW".equals(statement.text()) ? slowNanos.getAsLong() : 2_000_000L);
      }
    };
    scenario.bind(Scale.S, 1L);
    return scenario;
  }

  private static double metric(final Measurement m, final String name) {
    return m.perRep().stream().filter(x -> x.name().equals(name)).findFirst().orElseThrow().median();
  }

  private static final List<IssueScenario.Statement> UNORDERED = List.of(sql("SELECT x FROM T"));
  private static final List<IssueScenario.Statement> ORDERED   = List.of(sql("SELECT x FROM T ORDER BY x"));

  private static RatioScenario fake(final List<IssueScenario.Statement> statements, final List<String> slowRows,
      final List<String> referenceRows) {
    return new RatioScenario(1, 3.0, statements, statements, (scale, seed) -> List.of(Map.of()), (db, scale, seed) -> {
    }) {
      {
        bind(Scale.XS, 1L);
      }

      @Override
      protected List<String> rows(final RemoteDatabase db, final boolean slow, final int p, final int s) {
        return slow ? slowRows : referenceRows;
      }
    };
  }
}
