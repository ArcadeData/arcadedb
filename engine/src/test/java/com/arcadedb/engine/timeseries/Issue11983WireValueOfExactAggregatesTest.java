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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11983: the HTTP {@code /ts} query, the Grafana query and the gRPC TimeSeriesQuery serialized every
 * aggregate through {@link MultiColumnAggregationResult#getValue}, the nearest double, so MIN/MAX/SUM of a LONG column past 2^53
 * answered a different number than SQL over the same samples although the engine had computed it exactly (issues #9718, #9730).
 * {@link MultiColumnAggregationResult#getWireValue} is what they serialize now.
 */
class Issue11983WireValueOfExactAggregatesTest extends TestHelper {

  private static final long BIG = 9_007_199_254_740_993L; // 2^53 + 1, no double holds it

  @Test
  void anExactRequestAnswersTheExactNumberOnBothLayers() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts FIELDS (n LONG, d DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    database.begin();
    engine.appendSamples(new long[] { 1_000L, 2_000L, 3_000L }, new Object[] { BIG, 1L, Long.MAX_VALUE },
        new Object[] { 1.5, 2.5, 3.5 });
    database.commit();

    assertAnswers(engine);
    engine.compactAll();
    assertAnswers(engine);
  }

  private void assertAnswers(final TimeSeriesEngine engine) throws Exception {
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.MIN, "mn"),
        new MultiColumnAggregationRequest(1, AggregationType.MAX, "mx"),
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"),
        new MultiColumnAggregationRequest(2, AggregationType.SUM, "ds"),
        MultiColumnAggregationRequest.count("c"));

    // one bucket per sample: 1000 holds BIG, 2000 holds 1, 3000 holds Long.MAX_VALUE
    final MultiColumnAggregationResult perSample = engine.aggregateMulti(0L, 10_000L, requests, 1_000L, null);
    assertThat(perSample.getWireValue(1_000L, 0)).isEqualTo(BIG);
    assertThat(perSample.getWireValue(1_000L, 1)).isEqualTo(BIG);
    assertThat(perSample.getWireValue(1_000L, 2)).isEqualTo(BIG);
    assertThat(perSample.getWireValue(3_000L, 1)).isEqualTo(Long.MAX_VALUE);
    // a column that is not integral answers its double, as before
    assertThat(perSample.getWireValue(1_000L, 3)).isEqualTo(1.5);
    assertThat(perSample.getWireValue(1_000L, 4).longValue()).isEqualTo(1L);
    // a bucket nothing reached is the absent marker on the exact arm too
    assertThat(TimeSeriesNaN.isAbsent(perSample.getWireValue(5_000L, 1).doubleValue())).isTrue();

    // one bucket for all three: the total overflows a long and stays exact
    final MultiColumnAggregationResult all = engine.aggregateMulti(0L, 10_000L, requests, 1_000_000L, null);
    assertThat(all.getWireValue(0L, 0)).isEqualTo(1L);
    assertThat(all.getWireValue(0L, 1)).isEqualTo(Long.MAX_VALUE);
    assertThat(all.getWireValue(0L, 2)).isEqualTo(
        BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(BIG)).add(BigDecimal.ONE));
    // the double accessor is unchanged: the nearest double
    assertThat(all.getValue(0L, 1)).isEqualTo((double) Long.MAX_VALUE);
  }
}
