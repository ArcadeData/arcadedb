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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.ParallelScanProducerPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parallel type scans and what runs in their workers: filters, projections, aggregations (SQL and openCypher), DISTINCT,
 * ORDER BY ... LIMIT, index ranges handed to the workers, and the release of the producers when the consumer is done.
 * <p>
 * Every test builds its own fixture: the database is dropped after each test, so the database-level settings a test
 * changes die with it, and the JVM-wide state (the {@link PhysicalOrderRidFetcher} buffer cap, the logger) is restored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelScanExecutionTest extends TestHelper {
  private static final int ROWS = 20_000;

  @AfterEach
  void restoreBufferCap() {
    PhysicalOrderRidFetcher.maxBufferedRids = 1 << 20;
  }

  // ---- #8523: SQL aggregation used one core ----
  // A WHERE clause took a type scan off the parallel path, the projections and the aggregation ran on the one thread
  // consuming the scan, a type with a single bucket (the default) was never scanned in parallel, and a cached plan lost
  // the parallel scan its first execution had.
  // Every parallel answer here is checked against the same query run with parallel scans disabled - rows AND their order,
  // groups AND theirs - because the parallel scan promises the sequential scan's order. The values are integers (stored
  // as doubles too), so the sums are exact whatever order the workers add them in.

  private static final String[] MODES = { "AIR", "FOB", "MAIL", "RAIL", "SHIP", "TRUCK" };

  private static final String AGG_Q1  = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
      + "sum(l_extendedprice * (1 - l_discount)) AS sum_disc, avg(l_quantity) AS avg_qty, min(l_extendedprice) AS min_price, "
      + "max(l_shipdate) AS max_day, count(*) AS n FROM %s WHERE l_shipdate <= '1998-09-02' "
      + "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
  private static final String AGG_Q6  = "SELECT sum(l_extendedprice * l_discount) AS revenue, count(*) AS n FROM %s "
      + "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount BETWEEN 5 AND 7 AND l_quantity < 24";
  private static final String AGG_TOP = "SELECT l_partkey, sum(l_extendedprice * (1 - l_discount)) AS rev FROM %s "
      + "GROUP BY l_partkey ORDER BY rev DESC, l_partkey ASC LIMIT 10";

  private void createAggregationFixture() {
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

  /**
   * Regression test for issue #8523.
   * Question 1 of the issue: a WHERE clause no longer makes the scan sequential, and the rows keep their order.
   */
  @Test
  void filteredScanRunsInParallelInTheSequentialOrder() {
    createAggregationFixture();
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      final String query = "SELECT id, l_shipdate FROM " + type + " WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'";
      assertThat(explain(query)).contains("FETCH FROM TYPE " + type + " WITH FILTER (parallel)");
      final List<String> parallel = rows(query);
      assertThat(parallel).isNotEmpty().isEqualTo(sequentialRows(query));

      final String limited = query + " LIMIT 37";
      assertThat(rows(limited)).as("a LIMIT keeps the rows the sequential scan keeps").isEqualTo(sequentialRows(limited));
    }
  }

  /**
   * Regression test for issue #8523.
   * Question 3: a type with one bucket is split in page ranges, and returns its rows in the sequential order.
   */
  @Test
  void singleBucketIsScannedInParallelInTheSequentialOrder() {
    createAggregationFixture();
    final String query = "SELECT id FROM OneBucket";
    assertThat(explain(query)).contains("FETCH FROM TYPE OneBucket (parallel)");
    final List<String> parallel = rows(query);
    assertThat(parallel).hasSize(ROWS).isEqualTo(sequentialRows(query));
  }

  /**
   * Regression test for issue #8523.
   * Question 2: projections and aggregation run in the workers, and answer what the sequential aggregation answers.
   */
  @Test
  void aggregationRunsInTheWorkersAndMatchesTheSequentialOne() {
    createAggregationFixture();
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      for (final String template : new String[] { AGG_Q1, AGG_Q6, AGG_TOP }) {
        final String query = template.formatted(type);
        final List<String> parallel = new ArrayList<>();
        final String plan = rowsAndPlan(query, parallel);
        assertThat(plan).as(query).contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
        assertThat(parallel).as(query).isNotEmpty().isEqualTo(sequentialRows(query));
      }
    }
  }

  /**
   * Regression test for issue #8523.
   * Without an ORDER BY the groups come out in the order the scan first meets them, each with the non-aggregate
   * values of its first row, and a LIMIT keeps the first groups: the parallel aggregation must give the same answer.
   */
  @Test
  void groupOrderFirstRowValuesAndLimitMatchTheSequentialAggregation() {
    createAggregationFixture();
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      final String grouped = "SELECT l_partkey, id, count(*) AS n, sum(l_quantity) AS q FROM " + type + " GROUP BY l_partkey";
      assertThat(rows(grouped)).isEqualTo(sequentialRows(grouped));
      assertThat(explain(grouped)).contains("CALCULATE AGGREGATE PROJECTIONS (parallel)");

      final String limited = grouped + " LIMIT 7";
      assertThat(rows(limited)).hasSize(7).isEqualTo(sequentialRows(limited));
    }
  }

  /**
   * Regression test for issue #8523.
   * An aggregate that cannot merge partial results keeps the aggregation on the consumer, and stays correct.
   */
  @Test
  void nonMergeableAggregateAggregatesSequentially() {
    createAggregationFixture();
    final String query = "SELECT l_returnflag, median(l_quantity) AS m, count(*) AS n FROM OneBucket GROUP BY l_returnflag ORDER BY l_returnflag";
    assertThat(explain(query)).doesNotContain("(parallel:");
    assertThat(rows(query)).isEqualTo(sequentialRows(query));
  }

  /**
   * Regression test for issue #8523.
   * A filter and an aggregation that match nothing still answer the sequential way: one row with a zero count.
   */
  @Test
  void aggregationOverNoRowStillAnswersOneRow() {
    createAggregationFixture();
    final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM FourBuckets WHERE l_shipdate > '2020-01-01'";
    final List<String> rows = rows(query);
    assertThat(rows).isEqualTo(sequentialRows(query)).hasSize(1);
  }

  /**
   * Regression test for issue #8523.
   * The bug the cached plan had: a copy of it lost the parallel scan the first execution had.
   */
  @Test
  void aCachedPlanStaysParallel() {
    createAggregationFixture();
    final String query = "SELECT id FROM FourBuckets WHERE l_quantity >= 0";
    for (int i = 0; i < 3; i++) {
      final List<String> rows = new ArrayList<>();
      assertThat(rowsAndPlan(query, rows)).as("execution " + i).contains("WITH FILTER (parallel)");
      assertThat(rows).hasSize(ROWS);
    }
    // cached under the text its literals were extracted into (issue #8307)
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(db.getExecutionPlanCache().contains(db.getStatementCache().getParameterized(query).cacheKey())).isTrue();
  }

  /**
   * Regression test for issue #8523.
   * The plan a sequential execution cached must still run in parallel once parallel scans are enabled again.
   */
  @Test
  void aPlanCachedBySequentialExecutionsRunsInParallelLater() {
    createAggregationFixture();
    final String query = "SELECT id FROM FourBuckets WHERE l_quantity >= 0";
    assertThat(sequentialRows(query)).hasSize(ROWS);
    final List<String> rows = new ArrayList<>();
    assertThat(rowsAndPlan(query, rows)).contains("WITH FILTER (parallel)");
    assertThat(rows).hasSize(ROWS);
  }

  /**
   * Regression test for issue #8523.
   * Workers do not see a transaction's changes, so a scan inside one stays sequential - and sees them.
   */
  @Test
  void insideATransactionTheScanStaysSequentialAndSeesItsChanges() {
    createAggregationFixture();
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

  /**
   * Regression test for issue #8523.
   * A function the engine does not ship promises nothing about concurrent use: it keeps the scan sequential.
   */
  @Test
  void aUserFunctionKeepsItsFilterAndItsAggregationSequential() {
    createAggregationFixture();
    database.command("sql", "DEFINE FUNCTION t8523.keep 'return q >= 0' PARAMETERS [q] LANGUAGE js");

    final String filter = "SELECT id FROM FourBuckets WHERE `t8523.keep`(l_quantity) = true";
    assertThat(explain(filter)).contains("WITH FILTER").doesNotContain("(parallel)");
    assertThat(rows(filter)).hasSize(ROWS);

    final String aggregate = "SELECT sum(`t8523.keep`(l_quantity).asInteger()) AS n FROM FourBuckets";
    assertThat(explain(aggregate)).doesNotContain("(parallel:");
  }

  /**
   * Regression test for issue #8523.
   * The limit of groups one GROUP BY may hold in heap is enforced on the parallel aggregation too.
   */
  @Test
  void theGroupLimitIsEnforcedOnTheParallelAggregation() {
    createAggregationFixture();
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

  /**
   * Regression test for issue #8523.
   * Splitting a bucket can be turned off: one worker per bucket, and a type with one bucket is sequential again.
   */
  @Test
  void pageRangesCanBeDisabled() {
    createAggregationFixture();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 0);
    assertThat(explain("SELECT id FROM OneBucket")).doesNotContain("(parallel)");
    assertThat(explain("SELECT id FROM FourBuckets")).contains("(parallel)");
  }

  /**
   * Regression test for issue #8523.
   * With the split disabled, a type with fewer buckets than the threshold stays sequential even when a bucket is empty
   * (the unit count used to overflow and miss the empty bucket, making such a type look split).
   */
  @Test
  void disabledPageRangesKeepATypeUnderTheBucketThresholdSequential() {
    createAggregationFixture();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 0);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_MIN_BUCKETS, 4);
    try {
      database.getSchema().createDocumentType("ThreeBuckets", 3);
      database.transaction(() -> {
        database.newDocument("ThreeBuckets").set("id", 1).save();
        database.newDocument("ThreeBuckets").set("id", 2).save();
      });
      assertThat(explain("SELECT id FROM ThreeBuckets")).doesNotContain("(parallel)");
      assertThat(rows("SELECT id FROM ThreeBuckets")).hasSize(2);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_MIN_BUCKETS, 2);
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

  // ---- #8333: index ranges hand their aggregation and their record loads to the parallel scan workers ----
  // Follow-up after #8523 made scans parallel: with an index on the filtered property, a range served by the scan
  // fallback ran its projections and aggregation on the query thread, behind the index fetch, instead of in the workers of
  // the parallel scan; and a range served in physical order loaded its records one by one on that thread. An index being
  // there made the query slower than it was without it.
  // Now both branches hand the aggregation, and the conditions the index does not answer, to the workers of a parallel
  // scan: the full scan of the type, or the load of the sorted record addresses cut in slices. Every answer is checked
  // against the same query run sequentially - rows AND their order, groups AND theirs - and the values are integers, so
  // the sums are exact whatever order the workers add them in.

  // ~96% OF THE ROWS: THE SCAN BRANCH
  private static final String INDEX_RANGE_Q1    = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
      + "avg(l_quantity) AS avg_qty, min(l_extendedprice) AS min_price, max(l_shipdate) AS max_day, count(*) AS n FROM %s "
      + "WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
  // ~3.6% OF THE ROWS WITH CONDITIONS THE INDEX DOES NOT ANSWER: THE PHYSICAL-ORDER BRANCH, AND A FILTER STEP
  private static final String INDEX_RANGE_Q6    = "SELECT sum(l_extendedprice * l_discount) AS revenue, count(*) AS n FROM %s "
      + "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' AND l_discount BETWEEN 5 AND 7 AND l_quantity < 24";
  // GROUPS WITHOUT AN ORDER BY COME OUT IN THE ORDER THEY ARE FIRST MET, WITH THE VALUES OF THEIR FIRST ROW
  private static final String INDEX_RANGE_FIRST = "SELECT l_partkey, id, count(*) AS n, sum(l_quantity) AS q FROM %s "
      + "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' GROUP BY l_partkey";

  private void createIndexRangeFixture() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES AND FEW HUNDRED INDEX ENTRIES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    createIndexedLineItems("OneBucket", 1);
    createIndexedLineItems("FourBuckets", 4);
  }

  private void createIndexedLineItems(final String type, final int buckets) {
    database.getSchema().createDocumentType(type, buckets);
    database.command("sql", "CREATE PROPERTY " + type + ".l_shipdate STRING");
    final Random rnd = new Random(1);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument(type).set("id", i, "l_partkey", (long) (1 + rnd.nextInt(500)), "l_quantity", (double) (1 + rnd.nextInt(50)),
            "l_extendedprice", (double) rnd.nextInt(100_000), "l_discount", (double) rnd.nextInt(11),
            "l_returnflag", String.valueOf("ARN".charAt(rnd.nextInt(3))), "l_linestatus", String.valueOf("FO".charAt(rnd.nextInt(2))),
            "l_shipdate", String.format("199%d-%02d-%02d", 2 + rnd.nextInt(7), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28))).save();
    });
    database.command("sql", "CREATE INDEX ON " + type + " (l_shipdate) NOTUNIQUE");
  }

  /**
   * Regression test for issue #8333.
   * Question 1 of the follow-up: the scan branch hands the whole aggregation to the workers of the parallel scan.
   */
  @Test
  void scanBranchAggregatesInTheWorkers() {
    createIndexRangeFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = INDEX_RANGE_Q1.formatted(type);
      final List<String> parallel = new ArrayList<>();
      final String plan = rowsAndPlan(query, parallel);
      assertThat(plan).as(query).contains("served by full scan").contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
      assertThat(parallel).as(query).isNotEmpty().isEqualTo(indexRangeSequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
    }
  }

  /**
   * Regression test for issue #8333.
   * The physical-order branch loads its records in the workers, and they run the residual conditions too.
   */
  @Test
  void physicalOrderBranchLoadsAndAggregatesInTheWorkers() {
    createIndexRangeFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = INDEX_RANGE_Q6.formatted(type);
      final List<String> parallel = new ArrayList<>();
      final String plan = rowsAndPlan(query, parallel);
      assertThat(plan).as(query).contains("FILTER ITEMS WHERE").contains("entries matched), loaded in parallel")
          .contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
      assertThat(parallel).as(query).isNotEmpty().isEqualTo(indexRangeSequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void groupsComeOutAsTheSequentialLoadMeetsThem() {
    createIndexRangeFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = INDEX_RANGE_FIRST.formatted(type);
      final List<String> parallel = new ArrayList<>();
      assertThat(rowsAndPlan(query, parallel)).as(query).contains("loaded in parallel");
      assertThat(parallel).as(query).hasSizeGreaterThan(100).isEqualTo(indexRangeSequentialRows(query));
    }
  }

  /**
   * Regression test for issue #8333.
   * A range too large to hold at once is read chunk by chunk: the aggregation runs one parallel round per chunk, its
   * partial states carrying on from round to round, and the groups still come out in the sequential order.
   */
  @Test
  void rangeLargerThanTheBufferAggregatesRoundAfterRound() {
    createIndexRangeFixture();
    PhysicalOrderRidFetcher.maxBufferedRids = 100;
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String template : new String[] { INDEX_RANGE_Q6, INDEX_RANGE_FIRST }) {
        final String query = template.formatted(type);
        final List<String> parallel = new ArrayList<>();
        final String plan = rowsAndPlan(query, parallel);
        assertThat(plan).as(query).contains("loaded in parallel").contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
        assertThat(parallel).as(query).isNotEmpty().isEqualTo(indexRangeSequentialRows(query));
      }
    }
  }

  /**
   * Regression test for issue #8333.
   * A residual condition reading {@code $current} sees the row the worker evaluates it on.
   */
  @Test
  void residualConditionOnCurrentMatchesTheSequentialAnswer() {
    createIndexRangeFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM " + type
          + " WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' AND $current.l_quantity < 24";
      final List<String> parallel = new ArrayList<>();
      assertThat(rowsAndPlan(query, parallel)).as(query).contains("FILTER ITEMS WHERE").contains("(parallel:");
      assertThat(parallel).as(query).isEqualTo(indexRangeSequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
    }
  }

  /**
   * Regression test for issue #8333.
   * An execution that started parallel stays parallel to its last chunk: parallel scans turned off while it runs must
   * not make it stop at the chunk it was on, as if no entry were left.
   */
  @Test
  void disablingParallelScansMidQueryLosesNoChunk() {
    createIndexRangeFixture();
    PhysicalOrderRidFetcher.maxBufferedRids = 100;
    final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM OneBucket WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01'";
    final List<String> expected = indexRangeSequentialRows(query);

    final AtomicBoolean flipped = new AtomicBoolean();
    final AfterRecordReadListener listener = record -> {
      if (flipped.compareAndSet(false, true))
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
      return record;
    };
    database.getEvents().registerListener(listener);
    try {
      final List<String> rows = new ArrayList<>();
      assertThat(rowsAndPlan(query, rows)).contains("loaded in parallel");
      assertThat(flipped.get()).isTrue();
      assertThat(rows).isEqualTo(expected);
    } finally {
      database.getEvents().unregisterListener(listener);
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  /**
   * Regression test for issue #8333.
   * Rows sorted by an ORDER BY the index does not serve are loaded by the workers too, chunked or not.
   */
  @Test
  void rowsReSortedByAnotherPropertyAreLoadedInParallel() {
    createIndexRangeFixture();
    for (final int bufferCap : new int[] { 1 << 20, 100 }) {
      PhysicalOrderRidFetcher.maxBufferedRids = bufferCap;
      for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
        final String query = "SELECT id, l_shipdate FROM " + type + " WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' ORDER BY id";
        final List<String> parallel = new ArrayList<>();
        assertThat(rowsAndPlan(query, parallel)).as(query).contains("loaded in parallel");
        assertThat(parallel).as(query).hasSizeGreaterThan(500).isEqualTo(indexRangeSequentialRows(query))
            .isEqualTo(rowsWithoutTheIndex(query, type));
      }
    }
  }

  /**
   * Regression test for issue #8333.
   * The type check the index fetch leaves behind it runs in the workers: a sub-type answers only its own records.
   */
  @Test
  void subTypeIsCheckedInTheWorkers() {
    createIndexRangeFixture();
    database.command("sql", "CREATE DOCUMENT TYPE Special EXTENDS OneBucket");
    final Random rnd = new Random(2);
    database.transaction(() -> {
      for (int i = 0; i < 4_000; i++)
        database.newDocument("Special").set("id", ROWS + i, "l_quantity", 1.0, "l_shipdate",
            String.format("199%d-%02d-%02d", 2 + rnd.nextInt(7), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28))).save();
    });

    for (final String query : new String[] {
        "SELECT count(*) AS n, sum(l_quantity) AS q FROM OneBucket WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01'",
        "SELECT count(*) AS n, sum(l_quantity) AS q FROM Special WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01'" }) {
      final List<String> parallel = new ArrayList<>();
      assertThat(rowsAndPlan(query, parallel)).as(query).contains("FILTER ITEMS BY TYPE").contains("loaded in parallel")
          .contains("(parallel:");
      assertThat(parallel).as(query).isEqualTo(indexRangeSequentialRows(query));
    }
    long special = 0;
    try (final ResultSet rs = database.query("sql", "SELECT l_shipdate FROM Special")) {
      while (rs.hasNext()) {
        final String day = rs.next().getProperty("l_shipdate");
        if (day.compareTo("1994-01-01") >= 0 && day.compareTo("1994-04-01") < 0)
          ++special;
      }
    }
    assertThat(special).isPositive();
    assertThat(rows("SELECT count(*) AS n FROM Special WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01'"))
        .containsExactly("n=" + special + ";");
  }

  /**
   * Regression test for issue #8333.
   * Question 2 of the follow-up: the share of the type an index range gives way to the scan at falls with the workers
   * the scan would run on, since the index entries are read by one thread whatever the parallelism.
   */
  @Test
  void theShareTheIndexGivesWayAtFallsWithTheScanWorkers() {
    createIndexRangeFixture();
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<Integer> buckets = database.getSchema().getType("OneBucket").getBucketIds(true);
    // 0.6 of the records for a sequential scan, divided by (1 + workers) / 2
    assertThat(PhysicalOrderRidFetcher.scanThreshold(db, buckets, 1)).isBetween(11_999L, 12_001L);
    assertThat(PhysicalOrderRidFetcher.scanThreshold(db, buckets, 4)).isBetween(4_799L, 4_801L);
    assertThat(PhysicalOrderRidFetcher.scanThreshold(db, buckets, 18)).isBetween(1_263L, 1_265L);
    // Never above the whole type, whatever the setting
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY, 5F);
    try {
      assertThat(PhysicalOrderRidFetcher.scanThreshold(db, buckets, 1)).isEqualTo(ROWS);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY,
          GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY.getDefValue());
    }

    // The fixture is cut in many units, and the producer pool has at least 2 threads: the scan would be parallel
    assertThat(ParallelTypeScan.plannedWorkers(db, buckets)).isGreaterThan(1);

    // ~45% of the type: below the share for a sequential scan (60%), above it for 2 workers or more (40% at most)
    final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM OneBucket WHERE l_shipdate >= '1992-01-01' AND l_shipdate < '1995-03-01'";
    final List<String> parallel = new ArrayList<>();
    assertThat(rowsAndPlan(query, parallel)).contains("served by full scan").contains("(parallel:");
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      final List<String> sequential = new ArrayList<>();
      assertThat(rowsAndPlan(query, sequential)).contains("served by physical order").doesNotContain("(parallel");
      assertThat(parallel).isEqualTo(sequential);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  /**
   * Regression test for issue #8333.
   * Inside a transaction the load stays on the query thread and sees the transaction's own changes.
   */
  @Test
  void insideATransactionTheLoadStaysSequentialAndSeesItsChanges() {
    createIndexRangeFixture();
    final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM OneBucket WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01'";
    final long before = countOf(query);
    database.transaction(() -> {
      database.newDocument("OneBucket").set("id", -1, "l_quantity", 3.0, "l_shipdate", "1994-02-02").save();
      database.newDocument("OneBucket").set("id", -2, "l_quantity", 3.0, "l_shipdate", "1994-02-03").save();
      try (final ResultSet rs = database.query("sql", "SELECT FROM OneBucket WHERE l_shipdate = '1994-02-04' LIMIT 1")) {
        rs.next().getElement().orElseThrow().delete();
      }

      final List<String> inTx = new ArrayList<>();
      assertThat(rowsAndPlan(query, inTx)).contains("served by physical order").doesNotContain("loaded in parallel")
          .doesNotContain("(parallel:");
      assertThat(countOf(query)).isEqualTo(before + 1);
    });
    assertThat(countOf(query)).isEqualTo(before + 1);
  }

  private long countOf(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  /** What the query answers once the index is gone: the plain parallel scan, the answer the index must not change. */
  private List<String> rowsWithoutTheIndex(final String query, final String type) {
    database.command("sql", "DROP INDEX `" + type + "[l_shipdate]`");
    try {
      final List<String> rows = new ArrayList<>();
      assertThat(rowsAndPlan(query, rows)).doesNotContain("FETCH FROM INDEX");
      return rows;
    } finally {
      database.command("sql", "CREATE INDEX ON " + type + " (l_shipdate) NOTUNIQUE");
    }
  }

  private List<String> indexRangeSequentialRows(final String query) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      final List<String> rows = new ArrayList<>();
      assertThat(rowsAndPlan(query, rows)).doesNotContain("(parallel").doesNotContain("loaded in parallel");
      return rows;
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  // ---- shared by #8523 and #8333: SQL rows rendered as text, with the plan that produced them ----

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

  private static String render(final Object value) {
    // AVERAGES ARE THE ONE NON-INTEGRAL VALUE: A DIVISION OF EXACT SUMS, PRINTED AT A FIXED PRECISION
    return value instanceof Double d ? String.format("%.6f", d) : String.valueOf(value);
  }

  /** The plan EXPLAIN prints for the query, without running it. */
  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  // ---- #8594: a parallel scan nobody reads any more releases its producers ----
  // A parallel scan whose query no longer needs rows - a satisfied LIMIT, or a result set read in part and left open -
  // kept its producers parked on full channels, each holding a thread of the JVM-wide producer pool, until the result set
  // was closed or the abandonment timeout (10 minutes) passed. A few unclosed pages of a RID-paginated read took the whole
  // pool, and the next query waited for rows no producer was left to produce.
  // Every wait here is bounded by a hang detector far below the 10 minute abandonment timeout, which is left at its
  // default: a regression hangs until the detector fires.

  private static final String LIMIT_RELEASE_TYPE      = "Rating";
  // ONE BUCKET (THE DEFAULT), SPLIT IN UNITS OF MORE ROWS THAN A UNIT'S CHANNEL HOLDS (4096), SO A PRODUCER NOBODY
  // DRAINS PARKS ON ITS FULL CHANNEL: THE SHAPE OF THE REPORTER'S REPRO
  private static final int    LIMIT_RELEASE_RECORDS   = 100_000;
  private static final int    LIMIT_RELEASE_PAGE_SIZE = 5_000;

  private void createLimitReleaseFixture() {
    // UNITS OF 8 PAGES, ABOUT 13,000 ROWS EACH: THE ~60 PAGES OF THIS TYPE WOULD STAY ONE UNIT AT THE DEFAULT 32
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 8);
    database.getSchema().createDocumentType(LIMIT_RELEASE_TYPE);
    database.begin();
    for (int i = 0; i < LIMIT_RELEASE_RECORDS; i++) {
      database.newDocument(LIMIT_RELEASE_TYPE).set("id", i, "userId", (long) (i % 610), "movieId", (long) (i % 9742), "rating", (i % 10) / 2.0)
          .save();
      if (i % 10_000 == 9_999) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  /**
   * Regression test for issue #8594.
   * The reporter's pattern: every page read to its end and never closed. Before the fix the fifth page (on 8 cores)
   * waited for the abandonment timeout.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void unclosedRidPagesReadToTheirEndDoNotStallTheNextPage() {
    createLimitReleaseFixture();
    assertThat(explain("SELECT @rid AS rid, userId FROM " + LIMIT_RELEASE_TYPE + " WHERE @rid > #-1:-1 LIMIT " + LIMIT_RELEASE_PAGE_SIZE)
        .contains("(parallel)"))
        .as("the page query must take the parallel path, or this test proves nothing").isTrue();

    String last = "#-1:-1";
    long total = 0;
    int pages = 0;
    while (true) {
      // NEVER CLOSED, ON PURPOSE
      final ResultSet rs = database.query("sql",
          "SELECT @rid AS rid, userId FROM " + LIMIT_RELEASE_TYPE + " WHERE @rid > " + last + " LIMIT " + LIMIT_RELEASE_PAGE_SIZE);
      int n = 0;
      String lastRid = null;
      while (rs.hasNext()) {
        lastRid = rs.next().getProperty("rid").toString();
        n++;
      }
      ++pages;
      total += n;
      if (n < LIMIT_RELEASE_PAGE_SIZE)
        break;
      last = lastRid;
    }

    assertThat(total).isEqualTo(LIMIT_RELEASE_RECORDS);
    assertThat(pages).isEqualTo(LIMIT_RELEASE_RECORDS / LIMIT_RELEASE_PAGE_SIZE + 1);
  }

  /**
   * Regression test for issue #8594.
   * A LIMIT that has delivered its last row releases the scan's producers at once: the pool threads they held are free
   * again although the result set is never closed.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void satisfiedLimitReleasesTheProducersWithoutClose() throws Exception {
    createLimitReleaseFixture();
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();

    // THE POOL IS JVM-WIDE: A THREAD STILL HELD BY ANOTHER TEST'S SCAN WOULD READ BELOW AS ONE THIS TEST'S LIMIT KEPT
    Awaitility.await("the producer pool is idle before the test, or the count below is not this test's").atMost(30, TimeUnit.SECONDS)
        .until(() -> ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() == 0);

    for (int i = 0; i < maxThreads * 2; i++) {
      // NEVER CLOSED, ON PURPOSE
      // EXACTLY TEN next(), NO FINAL hasNext(): THE RELEASE MUST COME WITH THE LAST ROW, NOT WITH A LATER CALL
      final ResultSet rs = database.query("sql", "SELECT FROM " + LIMIT_RELEASE_TYPE + " LIMIT 10");
      for (int row = 0; row < 10; row++) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
      }
    }

    final long deadline = System.currentTimeMillis() + 20_000;
    while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() > 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(10);
    assertThat(ParallelScanProducerPool.getInstance().getPoolStats().activeThreads())
        .as("the producers of a satisfied LIMIT must not keep their pool threads until the result set is closed").isZero();
  }

  /**
   * Regression test for issue #8594.
   * Result sets read in part and left open, no LIMIT: nothing tells the engine they are done with, so their producers
   * stay parked until they are closed or abandoned. They take every thread of the producer pool here, and a scan, a
   * parallel aggregation and a parallel GROUP BY (with its parallel merge) still complete: a query never waits on a
   * unit no producer has taken, nor on a task the saturated pool has not started.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void queriesProgressWhileAbandonedScansHoldEveryProducerThread() throws Exception {
    createLimitReleaseFixture();
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();
    final String scan = "SELECT FROM " + LIMIT_RELEASE_TYPE;
    final String count = "SELECT count(*) AS c FROM " + LIMIT_RELEASE_TYPE + " WHERE rating >= 0";
    // ONE GROUP PER RECORD: 100,000 PARTIAL GROUPS, FAR ABOVE THE 16,384 FROM WHICH THE PARTIALS ARE MERGED IN PARALLEL TOO
    final String groupBy = "SELECT id, count(*) AS c FROM " + LIMIT_RELEASE_TYPE + " GROUP BY id";
    assertThat(explain(scan)).as("the scan must take the parallel path, or this test proves nothing").contains("(parallel)");
    assertThat(explain(count)).as("the count must aggregate in parallel, or this test proves nothing")
        .contains("CALCULATE AGGREGATE PROJECTIONS (parallel");
    assertThat(explain(groupBy)).as("the GROUP BY must aggregate in parallel, or this test proves nothing")
        .contains("CALCULATE AGGREGATE PROJECTIONS (parallel");

    final List<ResultSet> abandoned = new ArrayList<>();
    try {
      for (int i = 0; i < maxThreads; i++) {
        final ResultSet rs = database.query("sql", scan);
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        abandoned.add(rs);
      }

      final long deadline = System.currentTimeMillis() + 20_000;
      while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() < maxThreads
          && System.currentTimeMillis() < deadline)
        Thread.sleep(10);
      assertThat(ParallelScanProducerPool.getInstance().getPoolStats().activeThreads())
          .as("the abandoned scans must hold every producer thread, or this test proves nothing").isEqualTo(maxThreads);

      try (final ResultSet rs = database.query("sql", scan)) {
        long n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).isEqualTo(LIMIT_RELEASE_RECORDS);
      }

      try (final ResultSet rs = database.query("sql", count)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(LIMIT_RELEASE_RECORDS);
      }

      try (final ResultSet rs = database.query("sql", groupBy)) {
        long groups = 0;
        while (rs.hasNext()) {
          assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
          groups++;
        }
        assertThat(groups).isEqualTo(LIMIT_RELEASE_RECORDS);
      }
    } finally {
      for (final ResultSet rs : abandoned)
        rs.close();
    }
  }

  // ---- #10747: a cancelled parallel scan is not logged as a failure ----
  // Closing a parallel scan cancels its producers with an interrupt, and a producer interrupted while it waits for the I/O
  // lock of a page another thread is loading does not return, it throws (InterruptedIOException wrapped by the read lock as
  // "Error during read lock"). That throw was recorded as a failure of the scan and logged at WARNING with its stack trace,
  // for a cancellation the consumer itself asked for.
  // An after-read listener, which the bucket iterator runs under the same read lock as the page load, stands in for it: on
  // a producer thread it waits until it is interrupted and then throws exactly what the page load throws, so the interrupt
  // deterministically lands inside a read.

  private static final String CANCEL_TYPE    = "D";
  private static final int    CANCEL_RECORDS = 50_000;

  /** Regression test for issue #10747. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aProducerInterruptedInsideAReadIsNotReportedAsAFailure() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType(CANCEL_TYPE);
    database.transaction(() -> {
      for (int i = 0; i < CANCEL_RECORDS; i++)
        database.newDocument(CANCEL_TYPE).set("id", i).save();
    });

    // THE FIRST POSITION PAST THE FIRST TWO PAGES. A UNIT IS AT LEAST TWO PAGES, SO THOSE ARE ALWAYS IN THE FIRST UNIT, AND
    // EVERY OTHER UNIT STARTS PAST THEM. NOT A POSITION HALF WAY THROUGH THE BUCKET: A UNIT OF TWO FULL PAGES IS EXACTLY WHAT
    // ITS CHANNEL HOLDS, SO EACH PRODUCER PARKS ON THE FIRST UNIT IT TAKES, AND ON A POOL OF 4 THREADS (A 4-CORE BOX) NONE
    // EVER REACHED THE MIDDLE UNIT: THE STALL WAS LEFT TO THE LEFTOVER PRODUCERS OF A SCAN THAT FOUND THAT POSITION FIRST
    final long firstPagesEnd = 2L * ((LocalBucket) database.getSchema().getType(CANCEL_TYPE).getBuckets(false).getFirst()).getMaxRecordsInPage();

    final CountDownLatch stalled = new CountDownLatch(1);
    final CountDownLatch thrown = new CountDownLatch(1);
    final AfterRecordReadListener stall = record -> {
      // THE ROWS OF THE FIRST TWO PAGES PASS, SO THE CONSUMER GETS A ROW WHOEVER SCANS THE FIRST UNIT; A PRODUCER STALLS ON THE
      // FIRST ROW PAST THEM, THE FIRST OF ANY LATER UNIT OR THE ONE AFTER THE FIRST UNIT'S CHANNEL IS FULL, UNTIL CANCELLED
      if (record.getIdentity().getPosition() < firstPagesEnd || !(Thread.currentThread() instanceof ParallelScanProducerPool.ProducerThread))
        return record;
      stalled.countDown();
      try {
        Thread.sleep(60_000);
        return record;
      } catch (final InterruptedException e) {
        // WHAT PageManager.concurrentPageAccess THROWS, AS LocalDatabase.executeInReadLock WRAPS IT
        thrown.countDown();
        throw new DatabaseOperationException("Error during read lock",
            new InterruptedIOException("Interrupted while acquiring I/O lock for page"));
      }
    };

    final List<String> warnings = new CopyOnWriteArrayList<>();
    LogManager.instance().setLogger(new CapturingLogger(warnings));
    try {
      final String query = "SELECT FROM " + CANCEL_TYPE;
      try (final ResultSet plan = database.query("sql", "EXPLAIN " + query)) {
        assertThat(plan.next().<String>getProperty("executionPlanAsString")).as("the scan must run in parallel, or this test proves nothing")
            .contains("(parallel)");
      }

      // NO PARALLEL SCAN OF THIS DATABASE RUNS BEFORE THIS ONE, SO EVERY PRODUCER THE LISTENER STALLS IS ONE OF ITS OWN
      database.getEvents().registerListener(stall);
      final ResultSet rs = database.query("sql", query);
      try {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        assertThat(stalled.await(30, TimeUnit.SECONDS)).as("a producer must be inside the stalled read before the close").isTrue();
      } finally {
        rs.close();
      }
      assertThat(thrown.await(30, TimeUnit.SECONDS)).as("the close must interrupt the stalled producer").isTrue();

      // THE PRODUCER'S CATCH RUNS RIGHT AFTER THE THROW: WAIT FOR THE POOL TO GIVE ITS THREAD BACK
      final long deadline = System.currentTimeMillis() + 20_000;
      while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() > 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(10);

      assertThat(warnings).as("a cancellation the consumer asked for is not a failure of the scan").isEmpty();
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
      database.getEvents().unregisterListener(stall);
    }

    // THE SAME QUERY WITHOUT A STALL STILL ANSWERS EVERY ROW: THE CANCELLED SCAN LEFT NOTHING BEHIND
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + CANCEL_TYPE + " WHERE id >= 0")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(CANCEL_RECORDS);
    }
  }

  /** Records the WARNING and SEVERE lines of the parallel scan, the only ones the #10747 test is about. */
  private static final class CapturingLogger implements Logger {
    private final List<String> captured;

    private CapturingLogger(final List<String> captured) {
      this.captured = captured;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      record(requester, level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      record(requester, level, message);
    }

    private void record(final Object requester, final Level level, final String message) {
      if (requester instanceof ParallelTypeScan && level.intValue() >= Level.WARNING.intValue())
        captured.add(level + " " + message);
    }

    @Override
    public void flush() {
    }
  }

  // ---- #8797: openCypher aggregates over a label scan run in the parallel workers ----
  // An openCypher aggregate over a label scan ran on one core, while the same aggregate in SQL had run in the workers of a
  // parallel scan since #8523. An unfiltered label scan never took the parallel path, and the aggregation stayed on the
  // consuming thread even when it did. The aggregates that merge partial states (count, sum, avg, min, max) now aggregate
  // in the workers.
  // Every answer is checked against the same query run with parallel scans disabled - rows AND the order of the groups -
  // and the values are integers (stored as doubles too), so the sums are exact whatever order the workers add them in.

  private void createCypherAggregationFixture() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      database.getSchema().createVertexType(type, type.equals("OneBucket") ? 1 : 4);
      final Random rnd = new Random(3);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++) {
          final var vertex = database.newVertex(type).set("id", i, "grp", rnd.nextInt(100), "flag", rnd.nextInt(3), "x", (double) rnd.nextInt(1000),
              "n", (long) rnd.nextInt(50));
          // A FEW VERTICES WITHOUT x: NULLS ARE SKIPPED BY THE AGGREGATES
          if (i % 97 == 0)
            vertex.remove("x");
          vertex.save();
        }
      });
    }
  }

  /** Regression test for issue #8797. */
  @Test
  void wholeTypeAggregatesRunInTheWorkers() {
    createCypherAggregationFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String query : new String[] {
          "MATCH (n:%s) RETURN sum(n.x) AS s".formatted(type),
          "MATCH (n:%s) RETURN max(n.x) AS m".formatted(type),
          "MATCH (n:%s) RETURN min(n.x) AS m".formatted(type),
          "MATCH (n:%s) RETURN avg(n.x) AS a".formatted(type),
          "MATCH (n:%s) RETURN count(n.x) AS c".formatted(type),
          "MATCH (n:%s) RETURN count(*) AS c, sum(n.n) AS s, min(n.n) AS lo, max(n.n) AS hi".formatted(type),
          "MATCH (n:%s) WHERE n.x > 500 RETURN count(*) AS c, sum(n.x) AS s".formatted(type),
          "MATCH (n:%s) WHERE n.grp = 7 AND n.flag <> 1 RETURN count(n) AS c, avg(n.n) AS a".formatted(type),
          "MATCH (n:%s) WHERE n.grp IN [1, 2, 3] RETURN count(*) AS c".formatted(type),
          "MATCH (n:%s {grp: 5}) RETURN count(*) AS c, sum(n.x) AS s".formatted(type) }) {
        final String plan = cypherAssertSameAsSequential(query);
        assertThat(plan).as(query).contains("(parallel:");
      }
    }
  }

  /** Regression test for issue #8797. */
  @Test
  void groupedAggregatesRunInTheWorkersAndKeepTheFirstOccurrenceOrder() {
    createCypherAggregationFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String query : new String[] {
          "MATCH (n:%s) RETURN n.grp AS grp, count(*) AS c, sum(n.x) AS s".formatted(type),
          "MATCH (n:%s) RETURN n.grp AS grp, n.flag AS flag, count(*) AS c, max(n.x) AS m, min(n.id) AS first".formatted(type),
          "MATCH (n:%s) WHERE n.x > 500 RETURN n.grp AS grp, count(*) AS c".formatted(type),
          "MATCH (n:%s) RETURN n.flag AS flag, avg(n.n) AS a, count(n.x) AS c".formatted(type),
          "MATCH (n:%s) RETURN n.grp AS grp, count(*) AS c ORDER BY c DESC, grp LIMIT 5".formatted(type) }) {
        final String plan = cypherAssertSameAsSequential(query);
        assertThat(plan).as(query).contains("(parallel:");
      }
    }
  }

  /**
   * Regression test for issue #8797.
   * Issue #9047: a wrong argument count on a parallel aggregate is a semantic error, not a raw exception from the worker.
   */
  @Test
  void wrongAggregateArityOnTheParallelPathIsASemanticError() {
    createCypherAggregationFixture();
    for (final String query : new String[] { "MATCH (n:FourBuckets) RETURN sum() AS s", "MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN sum() AS s", "MATCH (n:FourBuckets) RETURN n.grp AS grp, count(n.x, n.n) AS c" })
      assertThatThrownBy(() -> cypherRows(query)).as(query).isInstanceOf(CommandSemanticException.class);
  }

  /**
   * Regression test for issue #8797.
   * What stays on the consuming thread - a DISTINCT aggregate, collect(), a function in an argument - still answers the same.
   */
  @Test
  void whatCannotMergeStaysSequentialAndCorrect() {
    createCypherAggregationFixture();
    for (final String query : new String[] {
        "MATCH (n:OneBucket) RETURN count(DISTINCT n.grp) AS c",
        "MATCH (n:OneBucket) RETURN n.flag AS flag, collect(n.n)[0] AS first",
        "MATCH (n:OneBucket) RETURN sum(toInteger(n.x)) AS s",
        "MATCH (n:OneBucket) RETURN count(*) * 10 + 1 AS c" }) {
      assertThat(cypherAssertSameAsSequential(query)).as(query).doesNotContain("(parallel:");
    }
  }

  /**
   * Regression test for issue #8797.
   * A filter that matches nothing answers what the sequential aggregation answers: a row with a zero count, or no group at all.
   */
  @Test
  void emptyInputAnswersLikeTheSequentialAggregation() {
    createCypherAggregationFixture();
    cypherAssertSameAsSequential("MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN count(*) AS c, sum(n.x) AS s, max(n.x) AS m, avg(n.x) AS a");
    cypherAssertSameAsSequential("MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN n.grp AS grp, count(*) AS c");
    assertThat(cypherRows("MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN n.grp AS grp, count(*) AS c")).isEmpty();
  }

  /**
   * Regression test for issue #8797.
   * The group cap applies to the groups of the parallel path as it does to the sequential one, and releases what the workers charged.
   */
  @Test
  void groupLimitFailsTheQueryLikeTheSequentialOne() {
    createCypherAggregationFixture();
    final long previous = database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10);
    try {
      final String query = "MATCH (n:FourBuckets) RETURN n.grp AS grp, count(*) AS c";
      assertThatThrownBy(() -> cypherRows(query)).isInstanceOf(CommandExecutionException.class);
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
      try {
        assertThatThrownBy(() -> cypherRows(query)).isInstanceOf(CommandExecutionException.class);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previous);
    }
    assertThat(cypherRows("MATCH (n:FourBuckets) RETURN n.grp AS grp, count(*) AS c")).hasSize(100);
  }

  /**
   * Regression test for issue #8797.
   * Workers do not see a transaction's changes, so inside one the aggregation stays on the caller - and sees them.
   */
  @Test
  void insideATransactionTheAggregationStaysSequentialAndSeesItsChanges() {
    createCypherAggregationFixture();
    database.transaction(() -> {
      database.newVertex("OneBucket").set("id", -1, "grp", 1_000, "x", 1_000_000.0).save();
      final List<String> rows = new ArrayList<>();
      final String plan = cypherRowsAndPlan("MATCH (n:OneBucket) RETURN max(n.x) AS m", rows);
      assertThat(plan).doesNotContain("(parallel:");
      assertThat(rows).containsExactly("m=1000000.0;");
    });
  }

  /**
   * Regression test for issue #8797.
   * A parameter in the filter and in the key is read by the workers like by the caller.
   */
  @Test
  void parametersReachTheWorkers() {
    createCypherAggregationFixture();
    final Map<String, Object> params = Map.of("min", 900);
    final List<String> parallel = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:FourBuckets) WHERE n.x >= $min RETURN n.flag AS flag, count(*) AS c", params)) {
      while (rs.hasNext())
        parallel.add(renderRow(rs.next()));
    }
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> sequential = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:FourBuckets) WHERE n.x >= $min RETURN n.flag AS flag, count(*) AS c", params)) {
      while (rs.hasNext())
        sequential.add(renderRow(rs.next()));
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
    assertThat(parallel).hasSize(3).isEqualTo(sequential);
  }

  /** Runs the Cypher query in parallel and sequentially, asserts the same rows in the same order, and returns the parallel run's plan. */
  private String cypherAssertSameAsSequential(final String query) {
    final List<String> parallel = new ArrayList<>();
    final String plan = cypherRowsAndPlan(query, parallel);

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> sequential = new ArrayList<>();
    try {
      assertThat(cypherRowsAndPlan(query, sequential)).doesNotContain("(parallel");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
    assertThat(parallel).as(query).isEqualTo(sequential);
    return plan;
  }

  private List<String> cypherRows(final String query) {
    final List<String> rows = new ArrayList<>();
    cypherRowsAndPlan(query, rows);
    return rows;
  }

  private String cypherRowsAndPlan(final String query, final List<String> rows) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(renderRow(rs.next()));
    }
    // PROFILE RUNS THE QUERY AGAIN AND REPORTS THE PLAN THAT RAN
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static String renderRow(final Result row) {
    final StringBuilder sb = new StringBuilder();
    for (final String p : row.getPropertyNames())
      sb.append(p).append('=').append(row.<Object>getProperty(p)).append(';');
    return sb.toString();
  }

  // ---- #8799: a plain SELECT DISTINCT runs in the parallel workers ----
  // SELECT DISTINCT over a type scan ran its projection and its dedup on the consuming thread (one core), while the same
  // answer as a GROUP BY aggregated in the parallel workers (13x faster). A plain DISTINCT is now planned as the GROUP BY
  // over its projected items; the rows and their first-occurrence order stay what the DISTINCT returned.

  private void createDistinctFixture() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      database.getSchema().createVertexType(type, type.equals("OneBucket") ? 1 : 4);
      final Random rnd = new Random(7);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex(type).set("id", i, "grp", rnd.nextInt(100), "x", rnd.nextDouble(), "s", "s" + rnd.nextInt(7)).save();
      });
    }
  }

  /** Regression test for issue #8799. */
  @Test
  void distinctRunsInTheWorkersAndKeepsFirstOccurrenceOrder() {
    createDistinctFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = "SELECT DISTINCT grp FROM " + type;
      assertThat(distinctPlan(query)).as(query).contains("CALCULATE AGGREGATE PROJECTIONS (parallel").doesNotContain("+ DISTINCT");
      assertThat(distinctRows(query)).as(query).hasSize(100).isEqualTo(firstOccurrences("SELECT grp AS v FROM " + type));
    }
  }

  /** Regression test for issue #8799. */
  @Test
  void filteredAndMultiColumnDistinct() {
    createDistinctFixture();
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      String query = "SELECT DISTINCT grp FROM " + type + " WHERE x > 0.5";
      assertThat(distinctRows(query)).as(query).isEqualTo(firstOccurrences("SELECT grp AS v FROM " + type + " WHERE x > 0.5"));

      query = "SELECT DISTINCT grp, s FROM " + type;
      assertThat(distinctRows(query)).as(query).hasSize(700).isEqualTo(firstOccurrences("SELECT grp + ',' + s AS v FROM " + type));

      query = "SELECT DISTINCT grp AS g FROM " + type;
      assertThat(distinctRows(query)).as(query).hasSize(100).isEqualTo(firstOccurrences("SELECT grp AS v FROM " + type));

      query = "SELECT count(*) AS c FROM (SELECT DISTINCT grp FROM " + type + ")";
      assertThat(distinctRows(query)).containsExactly("100");
    }
  }

  /**
   * Regression test for issue #8799.
   * What the rewrite must leave alone: a LIMIT stops a DISTINCT early, an ORDER BY sorts before the dedup, an aggregate is not a plain DISTINCT.
   */
  @Test
  void distinctThatIsNotPlainStaysADistinct() {
    createDistinctFixture();
    final String type = "FourBuckets";
    assertThat(distinctPlan("SELECT DISTINCT grp FROM " + type + " LIMIT 5")).contains("+ DISTINCT");
    assertThat(distinctRows("SELECT DISTINCT grp FROM " + type + " LIMIT 5")).hasSize(5);

    assertThat(distinctPlan("SELECT DISTINCT grp FROM " + type + " ORDER BY grp")).contains("+ DISTINCT");
    final List<String> sorted = distinctRows("SELECT DISTINCT grp FROM " + type + " ORDER BY grp");
    assertThat(sorted).hasSize(100).first().isEqualTo("0");

    assertThat(distinctPlan("SELECT DISTINCT grp % 10 FROM " + type)).contains("+ DISTINCT");
    assertThat(distinctRows("SELECT DISTINCT grp % 10 FROM " + type)).hasSize(10);
    assertThat(distinctPlan("SELECT DISTINCT * FROM " + type)).contains("+ DISTINCT");
    assertThat(distinctRows("SELECT DISTINCT * FROM " + type)).hasSize(ROWS);

    assertThat(distinctRows("SELECT DISTINCT grp FROM " + type + " SKIP 95")).hasSize(5);
  }

  /**
   * Regression test for issue #8799.
   * Found on the way: a GROUP BY on a computed expression, with no aggregate to project, answered null for that column.
   */
  @Test
  void groupByComputedExpressionWithoutAggregate() {
    createDistinctFixture();
    assertThat(distinctRows("SELECT grp % 10 AS m FROM OneBucket GROUP BY grp % 10")).hasSize(10).doesNotContain("null");
    assertThat(distinctRows("SELECT grp % 10 FROM OneBucket GROUP BY grp % 10")).hasSize(10).doesNotContain("null");
  }

  /**
   * Regression test for issue #8799.
   * Inside a transaction, and over a target that is not a type, the streaming DISTINCT stays: the GROUP BY would only block.
   */
  @Test
  void insideATransactionAndOverNonTypeTargetsTheDistinctStays() {
    createDistinctFixture();
    assertThat(distinctPlan("SELECT DISTINCT grp FROM (SELECT grp FROM OneBucket)")).contains("+ DISTINCT");
    assertThat(distinctRows("SELECT DISTINCT grp FROM (SELECT grp FROM OneBucket)")).hasSize(100);
    database.transaction(() -> {
      database.newVertex("OneBucket").set("id", -1, "grp", 1_000).save();
      assertThat(distinctPlan("SELECT DISTINCT grp FROM OneBucket")).contains("+ DISTINCT");
      assertThat(distinctRows("SELECT DISTINCT grp FROM OneBucket")).hasSize(101).contains("1000");
    });
  }

  /**
   * Regression test for issue #8799.
   * A per-record LET variable is not projected through the GROUP BY: the DISTINCT stays. And a plan is not reused across a transaction.
   */
  @Test
  void letVariablesAndPlanReuse() {
    createDistinctFixture();
    assertThat(distinctRows("SELECT DISTINCT $x AS v FROM FourBuckets LET $x = grp + 1")).hasSize(100).doesNotContain("null");
    final String query = "SELECT DISTINCT grp FROM FourBuckets";
    assertThat(distinctPlan(query)).contains("CALCULATE AGGREGATE PROJECTIONS");
    database.transaction(() -> assertThat(distinctPlan(query)).contains("+ DISTINCT"));
    assertThat(distinctPlan(query)).contains("CALCULATE AGGREGATE PROJECTIONS");
  }

  /**
   * Regression test for issue #8799.
   * The rewrite keeps the column names the DISTINCT returned, aliased or not.
   */
  @Test
  void columnNamesAreKept() {
    createDistinctFixture();
    for (final String query : new String[] { "SELECT DISTINCT grp AS m FROM OneBucket", "SELECT DISTINCT grp FROM OneBucket",
        "SELECT DISTINCT grp, s FROM OneBucket" }) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        names.addAll(rs.next().getPropertyNames());
      }
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
      try (final ResultSet rs = database.query("sql", query + " LIMIT 1000000")) {
        // WITH A LIMIT THE PLAN STAYS A DISTINCT: THE REFERENCE COLUMN NAMES
        assertThat(rs.next().getPropertyNames()).as(query).containsExactlyElementsOf(names);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    }
  }

  /**
   * Regression test for issue #8799.
   * Nulls, mixed numeric types and lists dedup as the DISTINCT dedups them.
   */
  @Test
  void valueShapesDedupLikeTheDistinct() {
    createDistinctFixture();
    database.getSchema().createVertexType("Shapes", 4);
    database.transaction(() -> {
      for (int i = 0; i < 4_000; i++) {
        final var v = database.newVertex("Shapes");
        switch (i % 6) {
        case 0 -> v.set("v", 1);
        case 1 -> v.set("v", 1L);
        case 2 -> v.set("v", 1.0);
        case 3 -> { }
        case 4 -> v.set("v", List.of(1, 2));
        default -> v.set("v", List.of(1, 2, i % 12));
        }
        v.save();
      }
    });
    final String query = "SELECT DISTINCT v FROM Shapes";
    assertThat(distinctRows(query)).containsExactlyElementsOf(distinctRows(query + " LIMIT 1000000"));
  }

  /**
   * Regression test for issue #8799.
   * Array and embedded values dedup exactly as the streaming DISTINCT (a LIMIT keeps the plan a DISTINCT) dedups them.
   */
  @Test
  void arrayValuesDedupByContent() {
    createDistinctFixture();
    database.getSchema().createVertexType("Arrays", 4);
    database.transaction(() -> {
      for (int i = 0; i < 4_000; i++)
        database.newVertex("Arrays").set("e", new float[] { i % 5, 1f }, "b", new byte[] { (byte) (i % 3) }, "o", Map.of("k", i % 4)).save();
    });
    for (final String column : new String[] { "e", "b", "o" })
      assertThat(distinctRows("SELECT DISTINCT " + column + " FROM Arrays")).as(column)
          .hasSameSizeAs(distinctRows("SELECT DISTINCT " + column + " FROM Arrays LIMIT 1000000"));
  }

  /**
   * Regression test for issue #8799.
   * GROUP BY keys that are not plain identifiers: record attributes, method calls, a key next to an aggregate and an ORDER BY.
   */
  @Test
  void groupByKeysThatAreNotPlainIdentifiers() {
    createDistinctFixture();
    assertThat(distinctRows("SELECT @type AS t, count(*) AS c FROM FourBuckets GROUP BY @type")).containsExactly("FourBuckets,20000");
    assertThat(distinctRows("SELECT s.toUpperCase() AS u, count(*) AS c FROM FourBuckets GROUP BY s.toUpperCase() ORDER BY u")).hasSize(7).first()
        .asString().startsWith("S0,");
    assertThat(distinctRows("SELECT grp % 10 AS m, count(*) AS c FROM FourBuckets GROUP BY grp % 10 ORDER BY m")).hasSize(10).first().asString()
        .startsWith("0,");
    assertThat(distinctRows("SELECT @rid AS r FROM FourBuckets GROUP BY @rid")).hasSize(ROWS);
  }

  /** Runs the query to its end and returns the plan that ran. */
  private String distinctPlan(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  /** The columns of each row, as comma-separated text. */
  private List<String> distinctRows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String p : row.getPropertyNames())
          sb.append(sb.isEmpty() ? "" : ",").append(row.<Object>getProperty(p));
        rows.add(sb.toString());
      }
    }
    return rows;
  }

  /** The distinct values of the query's {@code v} column, in the order they are first met by a plain scan. */
  private List<String> firstOccurrences(final String query) {
    final Set<String> seen = new LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        seen.add(String.valueOf(rs.next().<Object>getProperty("v")));
    }
    return new ArrayList<>(seen);
  }

  // ---- #8802: ORDER BY ... LIMIT keeps its best rows in the parallel workers ----
  // An ORDER BY bounded by LIMIT keeps its best rows in the workers of a parallel scan and merges them, instead of
  // projecting and sorting every row on the consumer thread. The answer must be the sequential one, ties included: among
  // equal keys the row the sequential scan meets first wins.

  private void createTopKFixture() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    for (final String[] t : new String[][] { { "OneBucket", "1" }, { "FourBuckets", "4" } }) {
      database.getSchema().createDocumentType(t[0], Integer.parseInt(t[1]));
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          // MANY TIES ON x (100 DISTINCT VALUES): THE TIE-BREAK IS WHAT THE SEQUENTIAL ORDER DECIDES
          database.newDocument(t[0]).set("id", i, "x", (i * 37) % 100, "grp", i % 7, "name", "n" + (i % 13)).save();
      });
    }
  }

  private List<String> topKRun(final String query, final boolean parallel, final String[] planOut) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    try (final ResultSet rs = database.query("sql", query)) {
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getPropertyNames().stream().sorted().map(n -> n + "=" + r.getProperty(n)).reduce("", (a, b) -> a + b + ";"));
      }
      if (planOut != null)
        planOut[0] = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      return rows;
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private void topKAssertSameAsSequential(final String query) {
    final String[] plan = new String[1];
    final List<String> parallel = topKRun(query, true, plan);
    assertThat(plan[0]).as(query).contains("parallel: ");
    assertThat(parallel).as(query).isNotEmpty().isEqualTo(topKRun(query, false, null));
  }

  /** Regression test for issue #8802. */
  @Test
  void topKRunsInTheWorkersAndKeepsTheSequentialAnswer() {
    createTopKFixture();
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      topKAssertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x DESC LIMIT 10");
      topKAssertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x ASC LIMIT 25");
      topKAssertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x DESC SKIP 1000 LIMIT 10");
      topKAssertSameAsSequential("SELECT id, x, grp FROM " + type + " ORDER BY grp ASC, x DESC LIMIT 40");
      topKAssertSameAsSequential("SELECT id, x FROM " + type + " WHERE grp < 4 ORDER BY x DESC LIMIT 10");
      topKAssertSameAsSequential("SELECT x AS y, id FROM " + type + " ORDER BY y DESC LIMIT 10");
      topKAssertSameAsSequential("SELECT id FROM " + type + " ORDER BY x ASC LIMIT 10");
      topKAssertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x DESC LIMIT " + (ROWS + 5));
    }
  }

  /** Regression test for issue #8802. */
  @Test
  void limitZeroAndEmptyFilterAnswerNothing() {
    createTopKFixture();
    assertThat(topKRun("SELECT id FROM FourBuckets ORDER BY x LIMIT 0", true, null)).isEmpty();
    assertThat(topKRun("SELECT id FROM FourBuckets WHERE x > 1000 ORDER BY x LIMIT 10", true, null)).isEmpty();
  }

  /**
   * Regression test for issue #8802.
   * Workers do not see a transaction's changes, so an ORDER BY inside one stays sequential - and sees them.
   */
  @Test
  void insideATransactionTheSortStaysSequentialAndSeesItsChanges() {
    createTopKFixture();
    database.begin();
    try {
      database.newDocument("OneBucket").set("id", -1, "x", 1_000, "grp", 0, "name", "tx").save();
      final String[] plan = new String[1];
      final List<String> rows = topKRun("SELECT id, x FROM OneBucket ORDER BY x DESC LIMIT 1", true, plan);
      assertThat(plan[0]).doesNotContain("parallel: ");
      assertThat(rows.getFirst()).contains("x=1000");
    } finally {
      database.rollback();
    }
  }

  /**
   * Regression test for issue #8802.
   * A function the engine does not ship promises nothing about concurrent use: it keeps the sort sequential.
   */
  @Test
  void aUserFunctionKeepsTheSortSequential() {
    createTopKFixture();
    database.command("sql", "DEFINE FUNCTION t8802.keep 'return q' PARAMETERS [q] LANGUAGE js");
    final String[] plan = new String[1];
    final List<String> rows = topKRun("SELECT id, `t8802.keep`(x) AS k FROM FourBuckets ORDER BY k DESC LIMIT 5", true, plan);
    assertThat(plan[0]).doesNotContain("parallel: ");
    assertThat(rows).hasSize(5);
  }
}
