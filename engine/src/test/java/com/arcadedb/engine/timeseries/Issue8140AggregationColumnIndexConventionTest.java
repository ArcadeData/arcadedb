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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8140: {@link MultiColumnAggregationRequest#columnIndex()} names a position in the ENGINE ROW -
 * {@code 0} is the timestamp and {@code 1 + <ordinal among non-TIMESTAMP columns>} is a value column - and both
 * halves of the push-down resolve it that way.
 * <p>
 * The record's javadoc has always said so; the mutable half ({@code TimeSeriesEngine.mutableSample}, which does
 * {@code row[columnIndex]}) read it that way and the sealed half
 * ({@code TimeSeriesSealedStore.aggregateMultiBlocks}, which fed it to the column decoder) read it as a SCHEMA
 * index. The two numbers are equal only while the TIMESTAMP column is declared FIRST, which issue #7702 stopped
 * being a property of every declaration {@code CREATE TIMESERIES TYPE} can spell, so the same samples answered
 * one number before compaction and another after it.
 * <p>
 * Every type below declares a column BEFORE the timestamp, and every assertion here is made twice - once while
 * the samples are still in the mutable bucket and once after {@code compactAll()} - because either half alone
 * passes against a fix that corrects only the other.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8140">issue #8140</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7899">issue #7899</a>, the same confusion on the read surfaces
 */
class Issue8140AggregationColumnIndexConventionTest extends TestHelper {

  private static final long   TS     = 1_700_000_000_000L;
  private static final long   HOUR   = 3_600_000L;
  private static final String TYPE   = "Aggr8140";

  /**
   * Schema order is {@code [v, host, ts, w]}; the engine row order is {@code [ts, v, host, w]}.
   * <ul>
   *   <li>{@code v} - schema index 0, row index 1: the two conventions disagree, and the SCHEMA reading points
   *       the mutable half at the timestamp, which is how five samples of 1..5 summed to 8.5e12;</li>
   *   <li>{@code w} - schema index 3, row index 3: the two conventions agree, so it is the control that says a
   *       fix has not merely shifted every index by one.</li>
   * </ul>
   */
  private void createSeries() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " FIELDS (v DOUBLE) TAGS (host STRING) TIMESTAMP ts FIELDS (w DOUBLE)");
    for (int i = 0; i < 5; i++)
      database.command("sql", "INSERT INTO " + TYPE + " SET ts = " + (TS + i * 1000L)
          + ", host = 'h', v = " + (i + 1) + ".0, w = " + (i + 1) * 10 + ".0");
  }

  private LocalTimeSeriesType tsType() {
    return (LocalTimeSeriesType) database.getSchema().getType(TYPE);
  }

  private List<ColumnDefinition> columns() {
    return tsType().getTsColumns();
  }

  // -------------------------------------------------------------------------------------------------
  // The SQL push-down - SelectExecutionPlanner.handleTimeSeriesAggregationPushDown
  // -------------------------------------------------------------------------------------------------

  /** The reporter's repro, asserted on both sides of the compaction it is triggered by. */
  @Test
  void theSqlPushDownAnswersTheSameNumbersBeforeAndAfterCompaction() throws Exception {
    createSeries();

    assertPushDown("mutable", 15.0, 5.0);
    tsType().getEngine().compactAll();
    assertPushDown("sealed", 15.0, 5.0);
  }

  /** The control column, whose row and schema indices coincide: it was right before and must stay right. */
  @Test
  void theSqlPushDownStillAnswersAColumnWhoseRowAndSchemaIndicesCoincide() throws Exception {
    createSeries();

    assertPushDownOnW("mutable", 150.0, 50.0);
    tsType().getEngine().compactAll();
    assertPushDownOnW("sealed", 150.0, 50.0);
  }

  /**
   * The shape that has no right answer under two conventions: half the samples sealed, half still mutable, one
   * bucket. {@code aggregateMulti} reads both layers in the same pass, so a disagreement shows up INSIDE a
   * single number rather than as two queries answering differently.
   */
  @Test
  void aPartlyCompactedSeriesAnswersOneNumberForOneBucket() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Split8140"
        + " FIELDS (v DOUBLE) TAGS (host STRING) TIMESTAMP ts");
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO Split8140 SET ts = " + (TS + i * 1000L) + ", host = 'h', v = " + (i + 1) + ".0");

    ((LocalTimeSeriesType) database.getSchema().getType("Split8140")).getEngine().compactAll();

    for (int i = 3; i < 5; i++)
      database.command("sql", "INSERT INTO Split8140 SET ts = " + (TS + i * 1000L) + ", host = 'h', v = " + (i + 1) + ".0");

    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, sum(v) AS s, max(v) AS m FROM Split8140 GROUP BY b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue())
          .as("3 sealed samples and 2 mutable ones are one sum, not the sum of two readings").isEqualTo(15.0);
      assertThat(((Number) row.getProperty("m")).doubleValue()).isEqualTo(5.0);
    }
  }

  // -------------------------------------------------------------------------------------------------
  // The engine contract itself - both halves of aggregateMulti
  // -------------------------------------------------------------------------------------------------

  /**
   * The record read straight, with no producer in the way: {@code columnIndex} 1 is the first non-timestamp
   * column whatever the declaration says, on the mutable half and on the sealed half alike.
   */
  @Test
  void aggregateMultiResolvesColumnIndexAgainstTheRowOnBothHalves() throws Exception {
    createSeries();

    final TimeSeriesEngine engine = tsType().getEngine();
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_v"),
        new MultiColumnAggregationRequest(3, AggregationType.SUM, "sum_w"));

    final long bucket = Math.floorDiv(TS, HOUR) * HOUR;

    MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null);
    assertThat(result.getValue(bucket, 0)).as("mutable: row index 1 is 'v', not the timestamp").isEqualTo(15.0);
    assertThat(result.getValue(bucket, 1)).as("mutable: row index 3 is 'w'").isEqualTo(150.0);

    engine.compactAll();

    result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null);
    assertThat(result.getValue(bucket, 0)).as("sealed: the same row index must name the same column").isEqualTo(15.0);
    assertThat(result.getValue(bucket, 1)).isEqualTo(150.0);
  }

  /**
   * {@code TimeSeriesEngine.aggregate} - the single-column path - counts NON-TIMESTAMP columns instead, and is
   * documented to. It was already right on both halves; pinned here so the two methods cannot be brought onto
   * one convention by accident, since with the timestamp declared first nothing else would notice.
   */
  @Test
  void theSingleColumnAggregateKeepsCountingNonTimestampColumns() throws Exception {
    createSeries();

    final TimeSeriesEngine engine = tsType().getEngine();
    assertThat(engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.SUM, HOUR, null).getValue(0))
        .as("mutable: ordinal 0 among non-timestamp columns is 'v'").isEqualTo(15.0);

    engine.compactAll();

    assertThat(engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.SUM, HOUR, null).getValue(0))
        .as("sealed: the same ordinal must name the same column").isEqualTo(15.0);
  }

  /**
   * Row position 0 is the timestamp, so no request that reads a column may carry it - and the sealed half now
   * refuses one by name rather than decoding the timestamp as if it were a measurement. That refusal is only
   * safe because the push-down never builds such a request: a TIMESTAMP column is DELTA_OF_DELTA encoded, so
   * {@code ColumnDefinition.isNumericallyAggregatable()} is false for it and the planner declines the whole
   * push-down. Pinned here because the refusal and the guard have to stay on the same side of each other.
   */
  @Test
  void theSqlPushDownDeclinesAnAggregationOverTheTimestampColumn() {
    createSeries();

    assertThat(columns().get(2).isNumericallyAggregatable())
        .as("the TIMESTAMP column is not a measurement on either storage layer").isFalse();
    assertThat(planOf("SELECT ts.timeBucket('1h', ts) AS b, max(v) AS m FROM " + TYPE + " GROUP BY b"))
        .as("a DOUBLE field is still pushed down").contains("AGGREGATE FROM TIMESERIES " + TYPE);
    assertThat(planOf("SELECT ts.timeBucket('1h', ts) AS b, max(ts) AS m FROM " + TYPE + " GROUP BY b"))
        .as("the timestamp column is not, so row position 0 never reaches the engine")
        .doesNotContain("AGGREGATE FROM TIMESERIES");
  }

  /** COUNT names no column, so the plan spells it as the query did rather than as a placeholder index. */
  @Test
  void theExplainedPlanSpellsACountWithoutAColumn() {
    createSeries();

    final String plan = planOf(
        "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c, sum(v) AS s FROM " + TYPE + " GROUP BY b");
    assertThat(plan).contains("AGGREGATE FROM TIMESERIES " + TYPE).contains("count(*)").contains("sum(col1)");
  }

  // -------------------------------------------------------------------------------------------------
  // The helper the four producers share
  // -------------------------------------------------------------------------------------------------

  /** The conversion every producer applies between the name it resolved and the request it builds. */
  @Test
  void theGatewayTurnsASchemaIndexIntoTheRowIndexTheRequestCarries() {
    createSeries();
    final List<ColumnDefinition> columns = columns();

    assertThat(TimeSeriesGateway.findColumnIndex("v", columns)).as("'v' is declared first").isEqualTo(0);
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 0)).as("and is returned in row position 1").isEqualTo(1);
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 1)).as("'host' follows it in the row").isEqualTo(2);
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 2)).as("the TIMESTAMP column is row position 0").isZero();
    assertThat(TimeSeriesGateway.aggregationRowIndex(columns, 3)).as("'w' is declared last and read last").isEqualTo(3);

    // The order the helper describes is the order the engine really returns, so this is not the test's opinion.
    assertThat(TimeSeriesGateway.columnNames(columns, null)).containsExactly("ts", "v", "host", "w");
  }

  // -------------------------------------------------------------------------------------------------
  // Plumbing
  // -------------------------------------------------------------------------------------------------

  private String planOf(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
    }
  }

  private void assertPushDown(final String half, final double expectedSum, final double expectedMax) {
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, sum(v) AS s, max(v) AS m FROM " + TYPE + " GROUP BY b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue())
          .as("%s: sum(v) must sum 'v', not the timestamps beside it", half).isEqualTo(expectedSum);
      assertThat(((Number) row.getProperty("m")).doubleValue())
          .as("%s: max(v) must read 'v'", half).isEqualTo(expectedMax);
    }
  }

  private void assertPushDownOnW(final String half, final double expectedSum, final double expectedMax) {
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, sum(w) AS s, max(w) AS m FROM " + TYPE + " GROUP BY b")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue()).as("%s: sum(w)", half).isEqualTo(expectedSum);
      assertThat(((Number) row.getProperty("m")).doubleValue()).as("%s: max(w)", half).isEqualTo(expectedMax);
    }
  }
}
