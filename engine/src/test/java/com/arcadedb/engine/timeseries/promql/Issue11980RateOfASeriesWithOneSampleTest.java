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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for issue #11980: {@code rate()}, {@code irate()} and {@code increase()} answered {@code 0} for a series with
 * one sample in the window, where Prometheus answers no sample for it. The zeros then moved every aggregation above them. The
 * expected values are what Prometheus v3.5.0 answers for the same samples.
 */
class Issue11980RateOfASeriesWithOneSampleTest extends TestHelper {

  private static final long T = 1_700_000_000_000L;

  @Test
  void aSeriesWithOneSampleInTheWindowAnswersNothing() {
    database.command("sql", "CREATE TIMESERIES TYPE cnt TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO cnt SET ts = " + T + ", host = 'a', value = 10.0");
      database.command("sql", "INSERT INTO cnt SET ts = " + (T + 30_000) + ", host = 'a', value = 40.0");
      database.command("sql", "INSERT INTO cnt SET ts = " + (T + 10_000) + ", host = 'b', value = 5.0");
    });

    final long at = T + 40_000;
    assertThat(byHost("rate(cnt[1m])", at)).containsOnlyKeys("a");
    assertThat(byHost("rate(cnt[1m])", at).get("a")).isCloseTo(50.0 / 60.0, within(1e-9));
    assertThat(byHost("irate(cnt[1m])", at)).containsExactly(Map.entry("a", 1.0));
    assertThat(byHost("increase(cnt[1m])", at)).containsExactly(Map.entry("a", 50.0));

    assertThat(single("avg(rate(cnt[1m]))", at)).isCloseTo(50.0 / 60.0, within(1e-9));
    assertThat(single("min(increase(cnt[1m]))", at)).isEqualTo(50.0);
    assertThat(single("count(rate(cnt[1m]))", at)).isEqualTo(1.0);

    // the other range functions still answer a single-sample series
    assertThat(byHost("count_over_time(cnt[1m])", at)).containsOnlyKeys("a", "b");
  }

  @Test
  void irateAnswersNothingWhenItsLastTwoSamplesShareATimestamp() {
    database.command("sql", "CREATE TIMESERIES TYPE dup TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO dup SET ts = " + T + ", host = 'a', value = 1.0");
      database.command("sql", "INSERT INTO dup SET ts = " + (T + 10_000) + ", host = 'a', value = 2.0");
      database.command("sql", "INSERT INTO dup SET ts = " + (T + 10_000) + ", host = 'a', value = 3.0");
      database.command("sql", "INSERT INTO dup SET ts = " + (T + 5_000) + ", host = 'b', value = 1.0");
      database.command("sql", "INSERT INTO dup SET ts = " + (T + 5_000) + ", host = 'b', value = 1.0");
    });

    final long at = T + 20_000;
    assertThat(byHost("irate(dup[1m])", at)).isEmpty();
    // rate() still has two distinct times for host a, none for host b
    assertThat(byHost("rate(dup[1m])", at)).containsOnlyKeys("a");
  }

  @Test
  void aRangeQueryHasNoPointAtAStepWhereTheSeriesHasOneSample() {
    database.command("sql", "CREATE TIMESERIES TYPE rng TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO rng SET ts = " + T + ", host = 'a', value = 10.0");
      database.command("sql", "INSERT INTO rng SET ts = " + (T + 30_000) + ", host = 'a', value = 40.0");
      database.command("sql", "INSERT INTO rng SET ts = " + (T + 10_000) + ", host = 'b', value = 5.0");
    });

    // steps at T+10s, +20s, +30s, +40s: host a has its second sample from T+30s on, host b never has one
    final MatrixResult matrix = (MatrixResult) new PromQLEvaluator((DatabaseInternal) database).evaluateRange(
        new PromQLParser("increase(rng[1m])").parse(), T + 10_000, T + 40_000, 10_000);

    assertThat(matrix.series()).hasSize(1);
    final MatrixSeries a = matrix.series().getFirst();
    assertThat(a.labels()).containsEntry("host", "a");
    assertThat(a.values()).hasSize(2);
    assertThat(a.values().get(0)[0]).isEqualTo((double) (T + 30_000));
    assertThat(a.values().get(1)[0]).isEqualTo((double) (T + 40_000));
    for (final double[] point : a.values())
      assertThat(point[1]).isGreaterThan(0.0);
  }

  /**
   * The evaluator asks {@link PromQLFunctions#answers} before it calls the function. A caller that does not must not get the
   * {@code 0} placeholder back: the functions apply the same rule and answer the absent marker.
   */
  @Test
  void theFunctionsThemselvesNeverAnswerAZeroNobodyMeasured() {
    final RangeSeries one = new RangeSeries(Map.of("host", "b"), List.of(new double[] { T, 5.0 }), T - 60_000, T);
    final RangeSeries sameTime = new RangeSeries(Map.of("host", "b"),
        List.of(new double[] { T - 10_000, 1.0 }, new double[] { T, 2.0 }, new double[] { T, 3.0 }), T - 60_000, T);

    for (final String function : List.of("rate", "increase", "irate"))
      assertThat(PromQLFunctions.answers(function, one)).as(function).isFalse();
    assertThat(PromQLFunctions.rate(one)).isNaN();
    assertThat(PromQLFunctions.increase(one)).isNaN();
    assertThat(PromQLFunctions.irate(one)).isNaN();

    // the last two samples share a timestamp: irate() has nothing to divide by, rate() still has two distinct times
    assertThat(PromQLFunctions.answers("irate", sameTime)).isFalse();
    assertThat(PromQLFunctions.irate(sameTime)).isNaN();
    assertThat(PromQLFunctions.answers("rate", sameTime)).isTrue();
    assertThat(PromQLFunctions.rate(sameTime)).isGreaterThan(0.0);
  }

  private Map<String, Double> byHost(final String query, final long atMs) {
    return evaluate(query, atMs).samples().stream()
        .collect(Collectors.toMap(s -> s.labels().get("host"), VectorSample::value));
  }

  private double single(final String query, final long atMs) {
    final InstantVector vector = evaluate(query, atMs);
    assertThat(vector.samples()).hasSize(1);
    return vector.samples().getFirst().value();
  }

  private InstantVector evaluate(final String query, final long atMs) {
    return (InstantVector) new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(new PromQLParser(query).parse(), atMs);
  }
}
