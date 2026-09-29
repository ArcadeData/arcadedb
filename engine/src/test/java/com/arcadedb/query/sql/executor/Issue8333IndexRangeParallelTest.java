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
import com.arcadedb.event.AfterRecordReadListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8333, follow-up after #8523 made scans parallel: with an index on the filtered property, a range served by the
 * scan fallback ran its projections and aggregation on the query thread, behind the index fetch, instead of in the
 * workers of the parallel scan; and a range served in physical order loaded its records one by one on that thread.
 * An index being there made the query slower than it was without it.
 * <p>
 * Now both branches hand the aggregation, and the conditions the index does not answer, to the workers of a parallel
 * scan: the full scan of the type, or the load of the sorted record addresses cut in slices. Every answer is checked
 * against the same query run sequentially - rows AND their order, groups AND theirs - and the values are integers, so
 * the sums are exact whatever order the workers add them in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8333IndexRangeParallelTest extends TestHelper {
  private static final int ROWS = 20_000;

  // ~96% OF THE ROWS: THE SCAN BRANCH
  private static final String Q1    = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
      + "avg(l_quantity) AS avg_qty, min(l_extendedprice) AS min_price, max(l_shipdate) AS max_day, count(*) AS n FROM %s "
      + "WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
  // ~3.6% OF THE ROWS WITH CONDITIONS THE INDEX DOES NOT ANSWER: THE PHYSICAL-ORDER BRANCH, AND A FILTER STEP
  private static final String Q6    = "SELECT sum(l_extendedprice * l_discount) AS revenue, count(*) AS n FROM %s "
      + "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' AND l_discount BETWEEN 5 AND 7 AND l_quantity < 24";
  // GROUPS WITHOUT AN ORDER BY COME OUT IN THE ORDER THEY ARE FIRST MET, WITH THE VALUES OF THEIR FIRST ROW
  private static final String FIRST = "SELECT l_partkey, id, count(*) AS n, sum(l_quantity) AS q FROM %s "
      + "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' GROUP BY l_partkey";

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES AND FEW HUNDRED INDEX ENTRIES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    createLineItems("OneBucket", 1);
    createLineItems("FourBuckets", 4);
  }

  @AfterEach
  void restoreBufferCap() {
    PhysicalOrderRidFetcher.maxBufferedRids = 1 << 20;
  }

  private void createLineItems(final String type, final int buckets) {
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

  /** Question 1 of the follow-up: the scan branch hands the whole aggregation to the workers of the parallel scan. */
  @Test
  void scanBranchAggregatesInTheWorkers() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = Q1.formatted(type);
      final List<String> parallel = new ArrayList<>();
      final String plan = rowsAndPlan(query, parallel);
      assertThat(plan).as(query).contains("served by full scan").contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
      assertThat(parallel).as(query).isNotEmpty().isEqualTo(sequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
    }
  }

  /** The physical-order branch loads its records in the workers, and they run the residual conditions too. */
  @Test
  void physicalOrderBranchLoadsAndAggregatesInTheWorkers() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = Q6.formatted(type);
      final List<String> parallel = new ArrayList<>();
      final String plan = rowsAndPlan(query, parallel);
      assertThat(plan).as(query).contains("FILTER ITEMS WHERE").contains("entries matched), loaded in parallel")
          .contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
      assertThat(parallel).as(query).isNotEmpty().isEqualTo(sequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
    }
  }

  @Test
  void groupsComeOutAsTheSequentialLoadMeetsThem() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = FIRST.formatted(type);
      final List<String> parallel = new ArrayList<>();
      assertThat(rowsAndPlan(query, parallel)).as(query).contains("loaded in parallel");
      assertThat(parallel).as(query).hasSizeGreaterThan(100).isEqualTo(sequentialRows(query));
    }
  }

  /**
   * A range too large to hold at once is read chunk by chunk: the aggregation runs one parallel round per chunk, its
   * partial states carrying on from round to round, and the groups still come out in the sequential order.
   */
  @Test
  void rangeLargerThanTheBufferAggregatesRoundAfterRound() {
    PhysicalOrderRidFetcher.maxBufferedRids = 100;
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String template : new String[] { Q6, FIRST }) {
        final String query = template.formatted(type);
        final List<String> parallel = new ArrayList<>();
        final String plan = rowsAndPlan(query, parallel);
        assertThat(plan).as(query).contains("loaded in parallel").contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
        assertThat(parallel).as(query).isNotEmpty().isEqualTo(sequentialRows(query));
      }
    }
  }

  /** A residual condition reading {@code $current} sees the row the worker evaluates it on. */
  @Test
  void residualConditionOnCurrentMatchesTheSequentialAnswer() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM " + type
          + " WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' AND $current.l_quantity < 24";
      final List<String> parallel = new ArrayList<>();
      assertThat(rowsAndPlan(query, parallel)).as(query).contains("FILTER ITEMS WHERE").contains("(parallel:");
      assertThat(parallel).as(query).isEqualTo(sequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
    }
  }

  /**
   * An execution that started parallel stays parallel to its last chunk: parallel scans turned off while it runs must
   * not make it stop at the chunk it was on, as if no entry were left.
   */
  @Test
  void disablingParallelScansMidQueryLosesNoChunk() {
    PhysicalOrderRidFetcher.maxBufferedRids = 100;
    final String query = "SELECT count(*) AS n, sum(l_quantity) AS q FROM OneBucket WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01'";
    final List<String> expected = sequentialRows(query);

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

  /** Rows sorted by an ORDER BY the index does not serve are loaded by the workers too, chunked or not. */
  @Test
  void rowsReSortedByAnotherPropertyAreLoadedInParallel() {
    for (final int bufferCap : new int[] { 1 << 20, 100 }) {
      PhysicalOrderRidFetcher.maxBufferedRids = bufferCap;
      for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
        final String query = "SELECT id, l_shipdate FROM " + type + " WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-04-01' ORDER BY id";
        final List<String> parallel = new ArrayList<>();
        assertThat(rowsAndPlan(query, parallel)).as(query).contains("loaded in parallel");
        assertThat(parallel).as(query).hasSizeGreaterThan(500).isEqualTo(sequentialRows(query)).isEqualTo(rowsWithoutTheIndex(query, type));
      }
    }
  }

  /** The type check the index fetch leaves behind it runs in the workers: a sub-type answers only its own records. */
  @Test
  void subTypeIsCheckedInTheWorkers() {
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
      assertThat(parallel).as(query).isEqualTo(sequentialRows(query));
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
   * Question 2 of the follow-up: the share of the type an index range gives way to the scan at falls with the workers
   * the scan would run on, since the index entries are read by one thread whatever the parallelism.
   */
  @Test
  void theShareTheIndexGivesWayAtFallsWithTheScanWorkers() {
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

  /** Inside a transaction the load stays on the query thread and sees the transaction's own changes. */
  @Test
  void insideATransactionTheLoadStaysSequentialAndSeesItsChanges() {
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
      assertThat(rowsAndPlan(query, rows)).doesNotContain("(parallel").doesNotContain("loaded in parallel");
      return rows;
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private static String render(final Object value) {
    // AVERAGES ARE THE ONE NON-INTEGRAL VALUE: A DIVISION OF EXACT SUMS, PRINTED AT A FIXED PRECISION
    return value instanceof Double d ? String.format("%.6f", d) : String.valueOf(value);
  }
}
