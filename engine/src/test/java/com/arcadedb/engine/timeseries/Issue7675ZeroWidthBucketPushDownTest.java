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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7675, the SQL sibling the sweep found: {@code ts.timeBucket('0s', ts)} answered TWO different things
 * depending on which plan the query got.
 * <p>
 * {@code SQLFunctionTimeBucket.execute} refuses a zero-width interval - "must be a positive amount of time",
 * added for issue #6388 because the division below it threw {@code ArithmeticException: / by zero}. The
 * aggregation push-down in {@code SelectExecutionPlanner} did not go through {@code execute}: it called
 * {@code parseInterval} directly, which carries no such guard, and handed the {@code 0} to
 * {@code TimeSeriesEngine.aggregateMulti}, whose {@code useFlatMode = bucketIntervalMs > 0} reads it as "one
 * bucket over the whole range". So the same query answered one row when the planner pushed it down and threw
 * when it did not - the same shape of divergence, one step further in, as the three wire protocols this issue
 * is about.
 */
class Issue7675ZeroWidthBucketPushDownTest extends TestHelper {

  private static final String TYPE = "ZeroBucketMetric";

  /**
   * The push-down-eligible shape: {@code ts.timeBucket} projected, GROUP BY on its alias, an aggregate
   * alongside, no residual WHERE. This is exactly the query {@link #aPositiveIntervalStillPushesDown} proves
   * reaches {@code AggregateFromTimeSeriesStep}, so the refusal below cannot be a query that was never
   * eligible in the first place.
   */
  private static String query(final String interval) {
    return "SELECT ts.timeBucket('" + interval + "', ts) AS tsBucket, sum(value) AS s"
        + " FROM " + TYPE + " GROUP BY tsBucket";
  }

  @Test
  void aZeroWidthBucketIsRefusedRatherThanCollapsingTheWholeRangeIntoOneRow() {
    createTypeWithSamples();

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", query("0s"))) {
        rs.stream().toList();
      }
    })
        .as("'0s' is not a bucketing, and answering one row over the whole range looks like a legitimate answer")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a positive amount of time");
  }

  /**
   * '0m' and '0h' parse through different arms of the unit switch and reach the same zero, so the refusal must
   * not be pinned to one spelling of it.
   */
  @Test
  void everySpellingOfAZeroWidthBucketIsRefusedAlike() {
    createTypeWithSamples();

    for (final String interval : new String[] { "0s", "0m", "0h", "0d", "0w" })
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", query(interval))) {
          rs.stream().toList();
        }
      })
          .as("interval '%s'", interval)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be a positive amount of time");
  }

  /**
   * The counter-case, and the one that proves the bail-out is narrow: a positive interval still takes the
   * push-down. Without this the fix above could have disabled the optimisation entirely and nothing would have
   * said so - the generic aggregation path answers the same numbers.
   */
  @Test
  void aPositiveIntervalStillPushesDown() {
    createTypeWithSamples();

    try (final ResultSet rs = database.query("sql", query("1h"))) {
      final String plan = rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
      assertThat(plan).as("a positive interval must still reach the TimeSeries push-down step")
          .contains("AGGREGATE FROM TIMESERIES " + TYPE);

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Double>getProperty("s")).isEqualTo(10.0);
      assertThat(rs.hasNext()).as("the three samples fall in one hour-wide bucket").isFalse();
    }
  }

  private void createTypeWithSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    database.transaction(() -> {
      try {
        tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L }, new Object[] { 4.0, 1.0, 5.0 });
      } catch (final Exception e) {
        throw new IllegalStateException("cannot append the test samples", e);
      }
    });
  }
}
