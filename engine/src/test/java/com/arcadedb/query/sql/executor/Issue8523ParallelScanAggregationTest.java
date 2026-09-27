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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8523: SQL aggregation used one core. A WHERE clause took a type scan off the parallel path, the projections
 * and the aggregation ran on the one thread consuming the scan, a type with a single bucket (the default) was never
 * scanned in parallel, and a cached plan lost the parallel scan its first execution had.
 * <p>
 * Every parallel answer here is checked against the same query run with parallel scans disabled - rows AND their
 * order, groups AND theirs - because the parallel scan promises the sequential scan's order. The values are integers
 * (stored as doubles too), so the sums are exact whatever order the workers add them in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8523ParallelScanAggregationTest extends TestHelper {
  private static final int      ROWS  = 20_000;
  private static final String[] MODES = { "AIR", "FOB", "MAIL", "RAIL", "SHIP", "TRUCK" };

  private static final String Q1 = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
      + "sum(l_extendedprice * (1 - l_discount)) AS sum_disc, avg(l_quantity) AS avg_qty, min(l_extendedprice) AS min_price, "
      + "max(l_shipdate) AS max_day, count(*) AS n FROM %s WHERE l_shipdate <= '1998-09-02' "
      + "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
  private static final String Q6 = "SELECT sum(l_extendedprice * l_discount) AS revenue, count(*) AS n FROM %s "
      + "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount BETWEEN 5 AND 7 AND l_quantity < 24";
  private static final String TOP = "SELECT l_partkey, sum(l_extendedprice * (1 - l_discount)) AS rev FROM %s "
      + "GROUP BY l_partkey ORDER BY rev DESC, l_partkey ASC LIMIT 10";

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    createLineItems("OneBucket", 1);
    createLineItems("FourBuckets", 4);
  }

  private void createLineItems(final String type, final int buckets) {
    database.getSchema().createDocumentType(type, buckets);
    final Random rnd = new Random(1);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument(type).set("id", i, "l_partkey", (long) (1 + rnd.nextInt(500)), "l_quantity", (double) (1 + rnd.nextInt(50)),
            "l_extendedprice", (double) rnd.nextInt(100_000), "l_discount", (double) rnd.nextInt(11),
            "l_returnflag", String.valueOf("ARN".charAt(rnd.nextInt(3))), "l_linestatus", String.valueOf("FO".charAt(rnd.nextInt(2))),
            "l_shipdate", String.format("199%d-%02d-%02d", 2 + rnd.nextInt(7), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28)),
            "l_shipmode", MODES[rnd.nextInt(MODES.length)]).save();
    });
  }

  /** Question 1 of the issue: a WHERE clause no longer makes the scan sequential, and the rows keep their order. */
  @Test
  void filteredScanRunsInParallelInTheSequentialOrder() {
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      final String query = "SELECT id, l_shipdate FROM " + type + " WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'";
      assertThat(plan(query)).contains("FETCH FROM TYPE " + type + " WITH FILTER (parallel)");
      final List<String> parallel = rows(query);
      assertThat(parallel).isNotEmpty().isEqualTo(sequentialRows(query));

      final String limited = query + " LIMIT 37";
      assertThat(rows(limited)).as("a LIMIT keeps the rows the sequential scan keeps").isEqualTo(sequentialRows(limited));
    }
  }

  /** Question 3: a type with one bucket is split in page ranges, and returns its rows in the sequential order. */
  @Test
  void singleBucketIsScannedInParallelInTheSequentialOrder() {
    final String query = "SELECT id FROM OneBucket";
    assertThat(plan(query)).contains("FETCH FROM TYPE OneBucket (parallel)");
    final List<String> parallel = rows(query);
    assertThat(parallel).hasSize(ROWS).isEqualTo(sequentialRows(query));
  }

  /** Question 2: projections and aggregation run in the workers, and answer what the sequential aggregation answers. */
  @Test
  void aggregationRunsInTheWorkersAndMatchesTheSequentialOne() {
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      for (final String template : new String[] { Q1, Q6, TOP }) {
        final String query = template.formatted(type);
        final List<String> parallel = new ArrayList<>();
        final String plan = rowsAndPlan(query, parallel);
        assertThat(plan).as(query).contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
        assertThat(parallel).as(query).isNotEmpty().isEqualTo(sequentialRows(query));
      }
    }
  }

  /**
   * Without an ORDER BY the groups come out in the order the scan first meets them, each with the non-aggregate
   * values of its first row, and a LIMIT keeps the first groups: the parallel aggregation must give the same answer.
   */
  @Test
  void groupOrderFirstRowValuesAndLimitMatchTheSequentialAggregation() {
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      final String grouped = "SELECT l_partkey, id, count(*) AS n, sum(l_quantity) AS q FROM " + type + " GROUP BY l_partkey";
      assertThat(rows(grouped)).isEqualTo(sequentialRows(grouped));
      assertThat(plan(grouped)).contains("CALCULATE AGGREGATE PROJECTIONS (parallel)");

      final String limited = grouped + " LIMIT 7";
      assertThat(rows(limited)).hasSize(7).isEqualTo(sequentialRows(limited));
    }
  }

  /** An aggregate that cannot merge partial results keeps the aggregation on the consumer, and stays correct. */
  @Test
  void nonMergeableAggregateAggregatesSequentially() {
    final String query = "SELECT l_returnflag, median(l_quantity) AS m, count(*) AS n FROM OneBucket GROUP BY l_returnflag ORDER BY l_returnflag";
    assertThat(plan(query)).doesNotContain("(parallel:");
    assertThat(rows(query)).isEqualTo(sequentialRows(query));
  }

  /** A filter and an aggregation that match nothing still answer the sequential way: one row with a zero count. */
  @Test
  void aggregationOverNoRowStillAnswersOneRow() {
    final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM FourBuckets WHERE l_shipdate > '2020-01-01'";
    final List<String> rows = rows(query);
    assertThat(rows).isEqualTo(sequentialRows(query)).hasSize(1);
  }

  /** The bug the cached plan had: a copy of it lost the parallel scan the first execution had. */
  @Test
  void aCachedPlanStaysParallel() {
    final String query = "SELECT id FROM FourBuckets WHERE l_quantity >= 0";
    for (int i = 0; i < 3; i++) {
      final List<String> rows = new ArrayList<>();
      assertThat(rowsAndPlan(query, rows)).as("execution " + i).contains("WITH FILTER (parallel)");
      assertThat(rows).hasSize(ROWS);
    }
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(query)).isTrue();
  }

  /** The plan a sequential execution cached must still run in parallel once parallel scans are enabled again. */
  @Test
  void aPlanCachedBySequentialExecutionsRunsInParallelLater() {
    final String query = "SELECT id FROM FourBuckets WHERE l_quantity >= 0";
    assertThat(sequentialRows(query)).hasSize(ROWS);
    final List<String> rows = new ArrayList<>();
    assertThat(rowsAndPlan(query, rows)).contains("WITH FILTER (parallel)");
    assertThat(rows).hasSize(ROWS);
  }

  /** Workers do not see a transaction's changes, so a scan inside one stays sequential - and sees them. */
  @Test
  void insideATransactionTheScanStaysSequentialAndSeesItsChanges() {
    database.transaction(() -> {
      database.newDocument("OneBucket").set("id", -1, "l_quantity", 1_000.0, "l_shipdate", "1994-06-01").save();

      final String query = "SELECT count(*) AS n, max(l_quantity) AS q FROM OneBucket WHERE l_shipdate >= '1994-01-01'";
      final List<String> rows = new ArrayList<>();
      final String plan = rowsAndPlan(query, rows);
      assertThat(plan).doesNotContain("(parallel");
      assertThat(rows.getFirst()).contains("1000");
      database.rollback();
    });
  }

  /** A function the engine does not ship promises nothing about concurrent use: it keeps the scan sequential. */
  @Test
  void aUserFunctionKeepsItsFilterAndItsAggregationSequential() {
    database.command("sql", "DEFINE FUNCTION t8523.keep 'return q >= 0' PARAMETERS [q] LANGUAGE js");

    final String filter = "SELECT id FROM FourBuckets WHERE `t8523.keep`(l_quantity) = true";
    assertThat(plan(filter)).contains("WITH FILTER").doesNotContain("(parallel)");
    assertThat(rows(filter)).hasSize(ROWS);

    final String aggregate = "SELECT sum(`t8523.keep`(l_quantity).asInteger()) AS n FROM FourBuckets";
    assertThat(plan(aggregate)).doesNotContain("(parallel:");
  }

  /** The limit of groups one GROUP BY may hold in heap is enforced on the parallel aggregation too. */
  @Test
  void theGroupLimitIsEnforcedOnTheParallelAggregation() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    try {
      assertThatThrownBy(() -> rows("SELECT l_partkey, count(*) FROM FourBuckets GROUP BY l_partkey"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Limit of allowed groups");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP,
          GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getDefValue());
    }
  }

  /** Splitting a bucket can be turned off: one worker per bucket, and a type with one bucket is sequential again. */
  @Test
  void pageRangesCanBeDisabled() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 0);
    assertThat(plan("SELECT id FROM OneBucket")).doesNotContain("(parallel)");
    assertThat(plan("SELECT id FROM FourBuckets")).contains("(parallel)");
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    rowsAndPlan(query, rows);
    return rows;
  }

  private String rowsAndPlan(final String query, final List<String> rows) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String p : row.getPropertyNames())
          sb.append(p).append('=').append(render(row.getProperty(p))).append(';');
        rows.add(sb.toString());
      }
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<String> sequentialRows(final String query) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      final List<String> rows = new ArrayList<>();
      assertThat(rowsAndPlan(query, rows)).doesNotContain("(parallel");
      return rows;
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private static String render(final Object value) {
    // AVERAGES ARE THE ONE NON-INTEGRAL VALUE: A DIVISION OF EXACT SUMS, PRINTED AT A FIXED PRECISION
    return value instanceof Double d ? String.format("%.6f", d) : String.valueOf(value);
  }
}
