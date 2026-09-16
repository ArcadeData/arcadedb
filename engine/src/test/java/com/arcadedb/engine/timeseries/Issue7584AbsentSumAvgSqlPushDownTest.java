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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7584, on the SQL push-down: the third reader of {@code MultiColumnAggregationResult.getValue()}.
 * <p>
 * Issue #7089 put SUM and AVG under the NaN-as-absent policy, so a bucket with no real sample for a column now
 * answers {@code NaN} rather than {@code 0.0}. Unlike the two HTTP handlers, {@code AggregateFromTimeSeriesStep}
 * sets the raw double as a result property instead of routing it through {@code putSampleValue}. That is still
 * correct at the response boundary, but only because {@link JSONObject#put(String, Object)} sends a {@code Number}
 * to the NaN-aware overload, which maps it to JSON {@code null} - where {@code JSONArray.put(Number)} would have
 * mapped it to {@code 0}. The coupling is implicit, so these tests pin both ends of it.
 */
class Issue7584AbsentSumAvgSqlPushDownTest extends TestHelper {

  private static final String TYPE  = "PushDownMetric";
  // ts.timeBucket takes the INTERVAL first and the timestamp column second, and the planner only pushes the
  // aggregation down when GROUP BY names the bucket alias. Get either wrong and the query still answers, from the
  // generic aggregation path - which is not the path this class is about, hence the plan assertion below.
  private static final String QUERY = "SELECT ts.timeBucket('1h', ts) AS tsBucket, sum(value) AS s, avg(value) AS a"
      + " FROM " + TYPE + " GROUP BY tsBucket";

  /**
   * Every sample in the single bucket is absent, so the push-down's SUM and AVG are absent too - and the JSON a
   * client receives carries {@code null}, not a measurement of zero.
   */
  @Test
  void anAllAbsentBucketPushesDownAsAbsentAndSerializesToJsonNull() {
    createTypeWithSamples(new long[] { 1_000L, 2_000L }, new Object[] { Double.NaN, Double.NaN });

    final Result row = singleRow();
    assertThat(row.<Double>getProperty("s")).as("SUM over no real sample is absent").isNaN();
    assertThat(row.<Double>getProperty("a")).as("AVG over no real sample is absent").isNaN();

    final JSONObject json = row.toJSON();
    assertThat(json.isNull("s")).as("the absent SUM must serialize to JSON null, never to 0").isTrue();
    assertThat(json.isNull("a")).as("the absent AVG must serialize to JSON null, never to 0").isTrue();
  }

  /**
   * The counter-case that makes the null above meaningful: real samples that cancel to zero stay the number zero
   * through both the push-down and the serializer.
   */
  @Test
  void aRealTotalOfZeroStaysTheNumberZero() {
    createTypeWithSamples(new long[] { 1_000L, 2_000L }, new Object[] { 2.5, -2.5 });

    final Result row = singleRow();
    assertThat(row.<Double>getProperty("s")).isEqualTo(0.0);

    final JSONObject json = row.toJSON();
    assertThat(json.isNull("s")).as("a real total of zero is data, not a gap").isFalse();
    assertThat(json.getDouble("s")).isEqualTo(0.0);
  }

  /**
   * And an absent sample among real ones is skipped rather than propagated: the SUM totals the real samples and
   * the AVG divides by their count.
   */
  @Test
  void absentSamplesAreSkippedRatherThanPoisoningThePushedDownTotal() {
    createTypeWithSamples(new long[] { 1_000L, 2_000L, 3_000L },
        new Object[] { 4.0, Double.NaN, 6.0 });

    final Result row = singleRow();
    assertThat(row.<Double>getProperty("s")).as("SUM of the real samples only").isEqualTo(10.0);
    assertThat(row.<Double>getProperty("a")).as("AVG divides by the 2 real samples, not by 3").isEqualTo(5.0);
  }

  /**
   * Runs {@link #QUERY} and returns its single bucket, having first proved the row came from
   * {@code AggregateFromTimeSeriesStep} rather than from the generic aggregation path, which would answer the same
   * query - with the same numbers - if the push-down declined it, and so would let this class pass without ever
   * touching the code it is named for.
   */
  private Result singleRow() {
    try (final ResultSet rs = database.query("sql", QUERY)) {
      final String plan = rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
      // AggregateFromTimeSeriesStep renders itself as this label, which is what the plan carries - the class
      // name never appears in prettyPrint() output.
      assertThat(plan).as("this test is about the TimeSeries push-down, so the plan must contain its step")
          .contains("AGGREGATE FROM TIMESERIES " + TYPE);

      assertThat(rs.hasNext()).as("the push-down must produce the one bucket the samples fall in").isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as("one bucket only").isFalse();
      return row;
    }
  }

  /**
   * A NaN sample has no SQL literal, so the samples go in through the engine rather than through INSERT.
   */
  private void createTypeWithSamples(final long[] timestamps, final Object[] values) {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    database.transaction(() -> {
      try {
        tsType.getEngine().appendSamples(timestamps, values);
      } catch (final Exception e) {
        throw new IllegalStateException("cannot append the test samples", e);
      }
    });
  }
}
