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
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7039 (follow-up on #4716): {@code min_over_time}/{@code max_over_time} and the
 * instant-vector MIN/MAX aggregation arms kept the {@code ±Infinity} seed the rest of the time-series stack had
 * already dropped, so an all-NaN window returned the sentinel as data.
 * <p>
 * {@code <} and {@code >} skip NaN implicitly, so an all-NaN input never displaced the seed. Prometheus - the
 * protocol being emulated - answers NaN here, and so does {@code TimeSeriesVectorOps.min} over the same samples:
 * the two paths disagreed with each other on identical data, which is what made this more than cosmetic.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7039PromQLMinMaxNaNTest extends TestHelper {

  private static final long EVAL_TIME_MS = 6_000L;

  @Test
  void minAndMaxOverTimeReportAnAllNaNWindowAsAbsent() throws Exception {
    createSeriesWithValues("promql7039_allnan", Double.NaN, Double.NaN);

    // Was +Infinity / -Infinity.
    assertThat(singleValueOf("min_over_time(promql7039_allnan[5m])")).as("min_over_time over an all-NaN window")
        .isNaN();
    assertThat(singleValueOf("max_over_time(promql7039_allnan[5m])")).as("max_over_time over an all-NaN window")
        .isNaN();
  }

  @Test
  void theMinAndMaxAggregationArmsReportAnAllNaNGroupAsAbsent() throws Exception {
    createSeriesWithValues("promql7039_agg", Double.NaN, Double.NaN);

    assertThat(singleValueOf("min(promql7039_agg)")).as("min() over an all-NaN group").isNaN();
    assertThat(singleValueOf("max(promql7039_agg)")).as("max() over an all-NaN group").isNaN();
  }

  @Test
  void aRealValueAmongTheNaNSamplesIsStillTheAnswer() throws Exception {
    createSeriesWithValues("promql7039_mixed", Double.NaN, 7.5);

    assertThat(singleValueOf("min_over_time(promql7039_mixed[5m])")).isEqualTo(7.5);
    assertThat(singleValueOf("max_over_time(promql7039_mixed[5m])")).isEqualTo(7.5);
  }

  /**
   * A genuine ±Infinity sample is a measurement, not the absent marker: it must survive the fold.
   */
  @Test
  void aRealInfinitySampleIsPreserved() throws Exception {
    createSeriesWithValues("promql7039_inf", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);

    assertThat(singleValueOf("min_over_time(promql7039_inf[5m])")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(singleValueOf("max_over_time(promql7039_inf[5m])")).isEqualTo(Double.POSITIVE_INFINITY);
  }

  private void createSeriesWithValues(final String typeName, final double first, final double second)
      throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    // A NaN value has no SQL literal, so the samples go in through the engine - the same write path the SQL
    // INSERT reaches, and the one the issue's repro uses.
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
    database.begin();
    tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L },
        new Object[] { "h1", "h1" }, new Object[] { first, second });
    database.commit();
  }

  private double singleValueOf(final String promql) {
    final PromQLExpr expr = new PromQLParser(promql).parse();
    final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(expr, EVAL_TIME_MS);
    assertThat(result).isInstanceOf(InstantVector.class);

    final InstantVector iv = (InstantVector) result;
    assertThat(iv.samples()).as("the query must produce exactly one sample: %s", promql).hasSize(1);
    final VectorSample sample = iv.samples().get(0);
    return sample.value();
  }
}
