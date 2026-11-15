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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11991: a Prometheus remote write ends a series with a staleness marker, the NaN with bits
 * {@code 0x7ff0000000000002}. It was read as an ordinary NaN measurement, so the ended series stayed in {@code up} as NaN for the
 * whole lookback, {@code sum(up)} became NaN and {@code count_over_time} counted the marker. The expected values are what
 * Prometheus v3.5.0 answers for the same samples, on the mutable layer and after compaction.
 */
class Issue11991StalenessMarkerTest extends TestHelper {

  private static final long   T     = 1_700_000_000_000L;
  /** Prometheus' value.StaleNaN, spelled out rather than read from the evaluator so the test pins the wire value. */
  private static final double STALE = Double.longBitsToDouble(0x7ff0000000000002L);

  @Test
  void aSeriesEndedByAStalenessMarkerLeavesTheVectorAndTheMarkerIsNoSample() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE up TIMESTAMP ts TAGS (job STRING) FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("up")).getEngine();
    database.begin();
    engine.appendSamples(new long[] { T, T + 15_000, T + 30_000, T, T + 15_000, T + 30_000, T + 45_000 },
        new Object[] { "a", "a", "a", "b", "b", "b", "b" },
        new Object[] { 1.0, 1.0, STALE, 1.0, 1.0, 1.0, 1.0 });
    // an ORDINARY NaN is a value, and Prometheus answers it as NaN
    engine.appendSamples(new long[] { T + 30_000 }, new Object[] { "c" }, new Object[] { Double.NaN });
    database.commit();

    assertPrometheusAnswers();
    engine.compactAll();
    assertPrometheusAnswers();
  }

  private void assertPrometheusAnswers() {
    // before the marker the series is there
    assertThat(byJob("up", T + 20_000)).containsOnlyKeys("a", "b");

    // from the marker on it is gone; the ordinary NaN of job c stays
    final Map<String, Double> at31 = byJob("up", T + 31_000);
    assertThat(at31).containsOnlyKeys("b", "c");
    assertThat(at31.get("c")).isNaN();
    assertThat(byJob("up{job=~\"a|b\"}", T + 46_000)).containsOnlyKeys("b");
    assertThat(single("count(up{job=~\"a|b\"})", T + 31_000)).isEqualTo(1.0);
    assertThat(single("sum(up{job=~\"a|b\"})", T + 31_000)).isEqualTo(1.0);

    // the marker is not a sample of any range
    assertThat(byJob("count_over_time(up[1m])", T + 31_000)).containsEntry("a", 2.0).containsEntry("b", 3.0);
    assertThat(byJob("max_over_time(up[1m])", T + 31_000)).containsEntry("a", 1.0);
    // a range holding nothing but the marker has no series for it
    assertThat(byJob("count_over_time(up[5s])", T + 31_000)).doesNotContainKey("a");
  }

  private Map<String, Double> byJob(final String query, final long atMs) {
    return evaluate(query, atMs).samples().stream().collect(Collectors.toMap(s -> s.labels().get("job"), VectorSample::value));
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
