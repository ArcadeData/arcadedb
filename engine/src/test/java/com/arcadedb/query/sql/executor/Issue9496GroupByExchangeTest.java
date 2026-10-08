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
import com.arcadedb.exception.HeapLimitExceededException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9496: a parallel GROUP BY with many keys hands the rows of the keys a worker does not hold to an exchange
 * partitioned by key, so each key is aggregated once instead of once per worker. The answer must be the sequential one:
 * the groups in the order the scan first meets them, each with the non-aggregate values of its first row, the same groups
 * under a LIMIT, and the same values (integers, so the sums are exact in any order).
 */
class Issue9496GroupByExchangeTest extends TestHelper {
  private static final int ROWS = 60_000;
  private static final int KEYS = 40_000;

  private final int  defaultExchangeMinGroups = AggregateProjectionCalculationStep.exchangeMinGroups;
  private final long budgetMb                 = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType("Item", 4);
    final Random rnd = new Random(9496);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Item").set("id", i, "k", (long) rnd.nextInt(KEYS), "s", "s" + rnd.nextInt(7), "q", rnd.nextInt(50),
            "p", (double) rnd.nextInt(100_000), "l", (long) rnd.nextInt(1_000_000)).save();
    });
  }

  @AfterEach
  void restoreSettings() {
    AggregateProjectionCalculationStep.exchangeMinGroups = defaultExchangeMinGroups;
    PhysicalOrderRidFetcher.maxBufferedRids = 1 << 20;
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(budgetMb);
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.reset();
  }

  /**
   * The partitions split the keys by the top bits of their hash times the golden ratio. With {@code hash % partitions} the
   * keys of a partition shared the low bits of their hash, the ones its {@link java.util.HashMap} picks a bucket with, so
   * dense integer keys over 16 partitions (4 workers, 4 partitions each) left 15 buckets in 16 empty, with chains of about
   * 10 keys (#9496).
   */
  @Test
  void partitionsSpreadDenseKeysOverTheBucketsOfTheirMaps() {
    for (final int partitions : new int[] { 2, 8, 12, 16, 64, 72 }) {
      final int[] sizes = new int[partitions];
      final List<List<Integer>> hashes = new ArrayList<>();
      for (int p = 0; p < partitions; p++)
        hashes.add(new ArrayList<>());
      for (long key = 0; key < 40_000; key++) {
        // THE HASH OF A ONE-VALUE GroupByKey
        final int hash = Arrays.hashCode(new Object[] { key });
        final int partition = AggregateProjectionCalculationStep.partitionOf(hash, partitions);
        assertThat(partition).isBetween(0, partitions - 1);
        ++sizes[partition];
        hashes.get(partition).add(hash);
      }
      final int even = 40_000 / partitions;
      for (int p = 0; p < partitions; p++) {
        assertThat(sizes[p]).as("partition %d of %d", p, partitions).isBetween(even - 10, even + 10);
        assertThat(averageChain(hashes.get(p))).as("partition %d of %d", p, partitions).isLessThan(2.5);
      }
    }
  }

  /**
   * What a worker holds queued for the exchange is not charged, so it must not grow with the square of the workers (4
   * partitions per worker, a batch per partition): full 64-row batches up to 8 workers, smaller ones past that, so a worker
   * queues at most 2,048 rows until the batch reaches its floor of 8 (past 64 workers).
   */
  @Test
  void queuedRowsPerWorkerStayBoundedWhateverTheWorkers() {
    for (int workers = 1; workers <= 256; workers++) {
      final int partitions = workers * 4;
      final int batch = AggregateProjectionCalculationStep.exchangeBatch(partitions);
      assertThat(batch).as("%d workers", workers).isBetween(8, 64);
      if (workers <= 8)
        assertThat(batch).as("%d workers", workers).isEqualTo(64);
      if (workers <= 64)
        assertThat((long) batch * partitions).as("%d workers", workers).isLessThanOrEqualTo(2_048);
    }
  }

  /** The average number of keys in a non-empty bucket of a {@link java.util.HashMap} holding these hashes. */
  private static double averageChain(final List<Integer> hashes) {
    int capacity = 16;
    while (hashes.size() > capacity * 0.75)
      capacity <<= 1;
    final boolean[] used = new boolean[capacity];
    int buckets = 0;
    for (final int h : hashes) {
      final int bucket = (h ^ (h >>> 16)) & (capacity - 1);
      if (!used[bucket]) {
        used[bucket] = true;
        ++buckets;
      }
    }
    return hashes.size() / (double) buckets;
  }

  /**
   * Nearly every row through the exchange: the groups keep the non-aggregate values of their earliest row (an Integer or a
   * Long of the same number, which are one key), NULL keys and NULL arguments count as in the sequential aggregation, and
   * expressions over the row are evaluated by the worker that read it.
   */
  @Test
  void everyShapeThroughTheExchangeMatchesTheSequentialAggregation() {
    database.getSchema().createDocumentType("Mixed", 4);
    final Random rnd = new Random(1);
    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++) {
        final int m = rnd.nextInt(5_000);
        database.newDocument("Mixed").set("id", i, "m", i % 9 == 0 ? null : (i % 2 == 0 ? (Object) m : (Object) (long) m),
            "t", "t" + rnd.nextInt(3), "v", i % 7 == 0 ? null : rnd.nextInt(100)).save();
      }
    });
    AggregateProjectionCalculationStep.exchangeMinGroups = 1;

    for (final String query : new String[] {
        "SELECT m, id, id * 2 AS d, count(*) AS n, count(v) AS cv, sum(v) AS sv, avg(v) AS av, min(v) AS lo, max(t) AS hi FROM Mixed GROUP BY m",
        "SELECT m, t, count(*) AS n, sum(v * 2) AS sv FROM Mixed GROUP BY m, t",
        "SELECT m, id, sum(v) AS sv FROM Mixed GROUP BY m LIMIT 100",
        "SELECT m, count(*) AS n FROM Mixed WHERE v < 50 GROUP BY m ORDER BY n DESC, m LIMIT 30" }) {
      final List<String> parallel = new ArrayList<>();
      assertThat(rowsAndPlan(query, parallel)).as(query).contains("groups by key exchange");
      assertThat(parallel).as(query).isNotEmpty().isEqualTo(sequentialRows(query));
    }
  }

  /**
   * A worker that flushes its groups to the shared ones (#9402) before it holds enough groups to exchange starts counting
   * again from zero; the exchange must still start, from the groups the worker ever created, or a GROUP BY of large groups
   * under a tight budget would flush round after round and never reach it. The shared groups and the exchange then both
   * hold groups, and both are merged and given back to the budget.
   */
  @Test
  void workersThatFlushedStillReachTheExchange() {
    database.getSchema().createDocumentType("Wide", 4);
    final String padding = "x".repeat(2_000);
    final Random rnd = new Random(2);
    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++)
        database.newDocument("Wide").set("id", i, "w", padding + rnd.nextInt(2_000), "v", rnd.nextInt(100)).save();
    });
    final String query = "SELECT w, id, count(*) AS n, sum(v) AS sv FROM Wide GROUP BY w";
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(10_000_000L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    final List<String> expected = sequentialRows(query);
    assertThat(expected).hasSize(2_000);

    // ONE UNIT PER BUCKET, SO 2 TO 4 WORKERS WHATEVER THE CORES. A GROUP IS CHARGED ABOUT 2.5 KB, AND A WORKER FLUSHES AT
    // max(1 MB, 16 MB / (4 x WORKERS)): ABOUT 800 GROUPS ON 2 WORKERS, 400 ON 4, BEFORE THE 900 THAT START THE EXCHANGE. THE
    // 2,000 GROUPS HELD BY BOTH THE SHARED SIDE AND THE EXCHANGE, PLUS A FLUSH'S WORTH PER WORKER, STILL FIT THE BUDGET
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 0);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(16L);
    AggregateProjectionCalculationStep.exchangeMinGroups = 900;
    final List<String> parallel = new ArrayList<>();
    assertThat(rowsAndPlan(query, parallel)).contains("groups by key exchange");
    assertThat(parallel).isEqualTo(expected);
    assertThat(QueryHeapBudget.getReservedBytes()).as("everything the query held is given back").isZero();
  }

  /** A query over the limit of groups is refused as the sequential one is, and gives back what the exchange charged. */
  @Test
  void limitOfGroupsIsEnforcedWhileExchanging() {
    final String query = "SELECT k, count(*) AS n FROM Item GROUP BY k";
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(20_000L);
    AggregateProjectionCalculationStep.exchangeMinGroups = 64;

    assertThatThrownBy(() -> rows(query)).isInstanceOf(HeapLimitExceededException.class);
    assertThat(QueryHeapBudget.getReservedBytes()).as("everything the query held is given back").isZero();

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      assertThatThrownBy(() -> rows(query)).isInstanceOf(HeapLimitExceededException.class);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  /**
   * An index range read chunk by chunk aggregates round after round (#8333): the workers keep their place in the exchange,
   * and the rows still queued after the last round join it, so the groups and their first rows are the sequential ones.
   */
  @Test
  void indexRangeReadRoundAfterRoundUsesTheExchange() {
    database.command("sql", "CREATE PROPERTY Item.id INTEGER");
    database.command("sql", "CREATE INDEX ON Item (id) UNIQUE");
    PhysicalOrderRidFetcher.maxBufferedRids = 100;
    AggregateProjectionCalculationStep.exchangeMinGroups = 16;
    final String query = "SELECT k, id, count(*) AS n, sum(q) AS sq FROM Item WHERE id >= 10000 AND id < 12000 GROUP BY k";
    final List<String> parallel = new ArrayList<>();
    assertThat(rowsAndPlan(query, parallel)).contains("loaded in parallel").contains("groups by key exchange");
    assertThat(parallel).hasSizeGreaterThan(1_000).isEqualTo(sequentialRows(query));
  }

  @Test
  void manyKeysMatchTheSequentialAggregation() {
    final String grouped = "SELECT k, id, sum(q) AS sq, sum(p * 2) AS sp, sum(l) AS sl, avg(q) AS aq, min(p) AS mn, max(s) AS mx, "
        + "count(*) AS n FROM Item GROUP BY k";
    assertThat(plan(grouped)).contains("CALCULATE AGGREGATE PROJECTIONS (parallel)");
    final List<String> parallel = new ArrayList<>();
    assertThat(rowsAndPlan(grouped, parallel)).contains("groups by key exchange");
    assertThat(parallel).hasSizeGreaterThan(30_000).isEqualTo(sequentialRows(grouped));

    for (final String query : new String[] { grouped + " LIMIT 5000", grouped + " ORDER BY sp DESC, k LIMIT 20",
        "SELECT k, s, count(*) AS n, sum(q) AS sq FROM Item WHERE q < 40 GROUP BY k, s",
        "SELECT s, count(*) AS n, sum(q) AS sq FROM Item GROUP BY s ORDER BY s" })
      assertThat(rows(query)).as(query).isNotEmpty().isEqualTo(sequentialRows(query));
  }

  /** The same query twice in a row: the second execution starts from a clean exchange. */
  @Test
  void repeatedExecutionsAgree() {
    final String query = "SELECT k, id, sum(q) AS sq, count(*) AS n FROM Item GROUP BY k";
    final List<String> first = rows(query);
    assertThat(rows(query)).isEqualTo(first).isEqualTo(sequentialRows(query));
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
        for (final String p : row.getPropertyNames()) {
          final Object v = row.getProperty(p);
          sb.append(p).append('=').append(v).append(':').append(v == null ? "null" : v.getClass().getSimpleName()).append(';');
        }
        rows.add(sb.toString());
      }
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<String> sequentialRows(final String query) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      assertThat(plan(query)).doesNotContain("(parallel");
      return rows(query);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
