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

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11990: an instant query answered a selector, and every aggregation and operator over it, at the
 * timestamp of the STORED sample each element came from ({@code count(up)} at T+46s stamped T+30s). Prometheus stamps every element
 * of an instant vector with the evaluation time.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11990InstantVectorAtEvaluationTimeTest extends TestHelper {

  private static final long   T     = 1_700_005_000_000L;
  /** Prometheus' value.StaleNaN, spelled out rather than read from the evaluator so the test pins the wire value. */
  private static final double STALE = Double.longBitsToDouble(0x7ff0000000000002L);

  @Test
  void everyElementOfAnInstantVectorIsAtTheEvaluationTime() {
    database.command("sql", "CREATE TIMESERIES TYPE g TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO g SET ts = " + T + ", host = 'a', value = 1.0");
      database.command("sql", "INSERT INTO g SET ts = " + (T - 10_000) + ", host = 'b', value = 2.0");
    });

    final long at = T + 30_500;
    for (final String query : new String[] { "g", "sum(g)", "count(g)", "g * 2", "g > 0", "-g", "abs(g)", "topk(1, g)",
        "g offset 1s", "max by (host) (g)", "g + g", "g > bool 1" }) {
      final InstantVector vector = evaluate(query, at);
      assertThat(vector.samples()).as(query).isNotEmpty();
      for (final VectorSample sample : vector.samples())
        assertThat(sample.timestampMs()).as(query).isEqualTo(at);
    }
  }

  /**
   * The stale-marker drop (issue #11991) and the evaluation-time stamp share one pass over the selector's answer: a series that
   * ended must stay out of the vector, and the live series next to it must still be stamped with the evaluation time, with and
   * without the compaction of the stored samples.
   */
  @Test
  void aSeriesEndedByAStalenessMarkerStaysOutAndTheLiveOnesAreAtTheEvaluationTime() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE up TIMESTAMP ts TAGS (job STRING) FIELDS (value DOUBLE) SHARDS 2");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("up")).getEngine();
    database.begin();
    engine.appendSamples(new long[] { T, T + 15_000, T, T + 15_000 }, new Object[] { "a", "a", "b", "b" },
        new Object[] { 1.0, STALE, 1.0, 1.0 });
    database.commit();

    assertEndedSeriesIsOutAndTheLiveOneIsAtEvaluationTime();
    engine.compactAll();
    assertEndedSeriesIsOutAndTheLiveOneIsAtEvaluationTime();
  }

  private void assertEndedSeriesIsOutAndTheLiveOneIsAtEvaluationTime() {
    final long at = T + 20_500;

    final InstantVector selector = evaluate("up", at);
    assertThat(selector.samples()).hasSize(1);
    assertThat(selector.samples().getFirst().labels()).containsEntry("job", "b");
    assertThat(selector.samples().getFirst().timestampMs()).isEqualTo(at);

    for (final String query : new String[] { "sum(up)", "count(up)", "up * 2", "-up", "abs(up)", "topk(2, up)",
        "max by (job) (up)" }) {
      final InstantVector vector = evaluate(query, at);
      assertThat(vector.samples()).as(query).hasSize(1);
      assertThat(vector.samples().getFirst().timestampMs()).as(query).isEqualTo(at);
    }
    // the ended series is no element of the aggregation either: a NaN from the marker would turn the sum into NaN
    assertThat(evaluate("sum(up)", at).samples().getFirst().value()).isEqualTo(1.0);
    assertThat(evaluate("count(up)", at).samples().getFirst().value()).isEqualTo(1.0);
  }

  private InstantVector evaluate(final String query, final long atMs) {
    return (InstantVector) new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(new PromQLParser(query).parse(), atMs);
  }
}
