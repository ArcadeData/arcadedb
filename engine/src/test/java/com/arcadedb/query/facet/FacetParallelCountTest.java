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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Parallel counting (plan 7 Task 8, {@code arcadedb.columnar.parallelCountTasks}): a facet's matches in a bucket are split into slices of
 * contiguous pages, counted over every segment by forked tasks on the query pool, each into a collector of its own, folded afterwards
 * (the buckets here have many segments, so a slice spans several of them). The answer must be the sequential one, byte for byte, for
 * every facet kind; a task the memory budget refuses runs on the caller; a caller that fails leaves no task running; a passed deadline
 * answers as the query thread alone; and a facet with fewer matches than {@code arcadedb.columnar.parallelMinMatches} in a bucket is
 * never forked.
 * <p>
 * Forked tasks are observed through {@link FacetExecutor#parallelCountHook}, counted by the test.
 */
class FacetParallelCountTest extends TestHelper {
  private static final String TYPE    = ColumnarTestData.TYPE;
  private static final long   BIN     = 1L << 20;
  private static final int    CHUNKS  = 16;
  /**
   * Every facet kind: HIERARCHY, DISTINCT (dictionary and numeric), STATS (LONG and DATETIME), terms by value, list and single-value
   * terms, numeric terms, a FILTER facet, range, histogram and the STATS of a DOUBLE column. Each is forked, except those
   * {@link #NOT_FORKED} names.
   */
  private static final String[] FACETS = { "t: tags HIERARCHY TOP 100", "d: brand DISTINCT", "n: fileSize DISTINCT", "s: fileSize STATS",
      "p: publishDate STATS", "bv: brand TERMS TOP 3 ORDER BY VALUE", "pd: publishDate TERMS TOP 2 ORDER BY VALUE", "b: brand TERMS TOP 50",
      "tg: tags TERMS TOP 100", "fs: fileSize TERMS TOP 20", "bf: brand TERMS TOP 10 FILTER fileSize < 1048576",
      "r: fileSize RANGES ([*, 1000000), [1000000, *))", "h: fileSize HISTOGRAM FIXED " + BIN, "sc: score STATS" };
  /**
   * The facets counted on the query thread alone: a histogram counts at finish, a hierarchy's node counts are not absorbed, and a DOUBLE
   * sum depends on the order it adds in.
   */
  private static final List<String> NOT_FORKED = List.of("t:", "h:", "sc:");

  private final AtomicInteger started  = new AtomicInteger();
  private final AtomicInteger ended    = new AtomicInteger();
  private final AtomicInteger segments = new AtomicInteger();

  private Object tasksBefore;
  private Object minMatchesBefore;

  @BeforeEach
  void keepSettings() {
    tasksBefore = GlobalConfiguration.COLUMNAR_PARALLEL_COUNT_TASKS.getValue();
    minMatchesBefore = GlobalConfiguration.COLUMNAR_PARALLEL_MIN_MATCHES.getValue();
    // the tasks are capped at the core count: with one core nothing forks and the fork assertions cannot hold (after the capture, so
    // the restore of a skipped test writes back the values found)
    assumeTrue(Runtime.getRuntime().availableProcessors() >= 2, "parallel counting needs at least 2 cores");
  }

  /** The settings as they were (a run may set them for the whole JVM), and no hook. */
  @AfterEach
  void restoreSettings() {
    FacetExecutor.parallelCountHook = null;
    GlobalConfiguration.COLUMNAR_PARALLEL_COUNT_TASKS.setValue(tasksBefore);
    GlobalConfiguration.COLUMNAR_PARALLEL_MIN_MATCHES.setValue(minMatchesBefore);
  }

  /**
   * The tag tree, then {@code assets} assets over {@code buckets} buckets in 16 chunks, each applied and flushed (flushMaxRows 4096)
   * before the next, so each bucket has many segments (mergeMaxSegments 64 keeps them).
   */
  private void load(final int assets, final int buckets) {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS, 4096);
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 64);
    final ColumnarTestData data = new ColumnarTestData(7L);
    data.tagHierarchy = AdvancedFacetsFixture.hierarchyJSON();
    data.maxTags = 3;
    data.doubleColumn = true;
    data.buckets = buckets;
    data.createSchema(database);
    final List<String> values = new ArrayList<>(AdvancedFacetsFixture.createTagTree(database));
    values.addAll(AdvancedFacetsFixture.ORPHANS);
    data.tagValues = values;
    for (int chunk = 0; chunk < CHUNKS; chunk++) {
      data.insertAssets(database, assets / CHUNKS);
      FacetOracle.awaitCaughtUp(database);
      for (final int bucketId : bucketIds())
        ColumnarTestAccess.maintainNow(manager(), bucketId);
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private int[] bucketIds() {
    return database.getSchema().getType(TYPE).getBuckets(false).stream().mapToInt(b -> b.getFileId()).toArray();
  }

  /** Counts the forked tasks that started, ended, and the segments they counted. */
  private void countTasks() {
    FacetExecutor.parallelCountHook = event -> {
      switch (event) {
      case FacetExecutor.TASK_START -> started.incrementAndGet();
      case FacetExecutor.TASK_SEGMENT -> segments.incrementAndGet();
      case FacetExecutor.TASK_END -> ended.incrementAndGet();
      default -> {
      }
      }
    };
  }

  private String facetsJSON(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rs.next();
      return rs.getFacets().toJSON().toString();
    }
  }

  private static void parallel(final int tasks, final long minMatches) {
    GlobalConfiguration.COLUMNAR_PARALLEL_COUNT_TASKS.setValue(tasks);
    GlobalConfiguration.COLUMNAR_PARALLEL_MIN_MATCHES.setValue(minMatches);
  }

  @Test
  @Tag("slow")
  @Timeout(600)
  void parallelCountEqualsSequentialForEveryFacetKind() {
    load(300_000, 2);
    for (final int bucketId : bucketIds())
      assertThat(ColumnarTestAccess.publishedSegments(manager(), bucketId)).as("segments of bucket " + bucketId).isGreaterThanOrEqualTo(4);
    countTasks();
    final List<String> clauses = new ArrayList<>(List.of(FACETS));
    final String combined = String.join(", ", FACETS);
    clauses.add(combined);
    for (final String where : new String[] { "", " WHERE fileSize > 0", " WHERE brand <> 'brand-1'" })
      for (final String clause : clauses) {
        final String sql = "SELECT FROM " + TYPE + where + " FACETS " + clause + " LIMIT 5";
        parallel(1, 1);
        final String sequential = facetsJSON(sql);
        assertThat(started.get()).as("no task forked with parallelCountTasks 1").isZero();

        parallel(4, 1);
        final String forked = facetsJSON(sql);
        if (!clause.equals(combined) && NOT_FORKED.contains(clause.substring(0, clause.indexOf(':') + 1)))
          assertThat(started.get()).as("tasks forked for '" + clause + "'").isZero();
        else
          assertThat(started.get()).as("tasks forked for '" + clause + "'" + where).isPositive();
        assertThat(ended.get()).isEqualTo(started.get());
        assertThat(forked).as("parallel answer of '" + clause + "'" + where).isEqualTo(sequential);
        assertThat(forked).doesNotContain(FacetResult.UNAVAILABLE);
        started.set(0);
        ended.set(0);
      }
  }

  /**
   * A forked task whose collector the memory budget refuses is not forked: its segments are counted on the caller, exactly. Terms
   * facets only: each reserves its counter when it begins a bucket (a STATS collector, for one, reserves nothing there).
   */
  @Test
  @Timeout(300)
  void refusedTaskBudgetRunsOnTheCaller() {
    load(30_000, 1);
    final String sql = "SELECT FROM " + TYPE + " FACETS b: brand TERMS TOP 50, tg: tags TERMS TOP 100, fs: fileSize TERMS TOP 20 LIMIT 5";
    parallel(1, 1);
    final String sequential = facetsJSON(sql);

    parallel(4, 1);
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final FacetMemoryBudget.Reservation[] held = new FacetMemoryBudget.Reservation[1];
    final AtomicInteger refusedForks = new AtomicInteger();
    FacetExecutor.parallelCountHook = event -> {
      switch (event) {
      // the whole budget is taken while the tasks' collectors reserve theirs, and given back right after
      case FacetExecutor.FORK_RESERVE -> held[0] = budget.reserve(budget.available(), 0);
      case FacetExecutor.FORK_RESERVED -> {
        if (held[0] != null) {
          held[0].close();
          held[0] = null;
          refusedForks.incrementAndGet();
        }
      }
      case FacetExecutor.TASK_START -> started.incrementAndGet();
      default -> {
      }
      }
    };
    final String forked = facetsJSON(sql);
    assertThat(refusedForks.get()).as("forks whose task collectors were refused").isPositive();
    assertThat(started.get()).as("no task runs without its collector").isZero();
    assertThat(forked).isEqualTo(sequential);
    assertThat(forked).doesNotContain(FacetResult.UNAVAILABLE);
  }

  /**
   * The budget runs out while the slices are counted (taken once the tasks are submitted, given back once every slice is counted): the
   * slice collectors, which never wait, are refused as they grow, and their slices are counted again by the facet's collector once
   * the budget is back. The facet's collector reserves nothing while the slices are counted, so the answer is the sequential one and no
   * facet is unavailable (a collector of the query thread growing then would wait out queryBudgetWait and be refused).
   */
  @Test
  @Timeout(300)
  void budgetRefusedWhileCountingFallsBackToTheSequentialAnswer() {
    load(30_000, 1);
    final Object waitBefore = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(200L);
    try {
      final String sql = "SELECT FROM " + TYPE + " FACETS fs: fileSize TERMS TOP 20, n: fileSize DISTINCT, b: brand TERMS TOP 50,"
          + " r: fileSize RANGES ([*, 1000000), [1000000, *)), s: fileSize STATS LIMIT 5";
      parallel(1, 1);
      final String sequential = facetsJSON(sql);

      parallel(4, 1);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final FacetMemoryBudget.Reservation[] held = new FacetMemoryBudget.Reservation[1];
      final AtomicInteger squeezed = new AtomicInteger();
      FacetExecutor.parallelCountHook = event -> {
        switch (event) {
        case FacetExecutor.FORK_SUBMITTED -> {
          held[0] = budget.reserve(budget.available(), 0);
          squeezed.incrementAndGet();
        }
        case FacetExecutor.FORK_COUNTED -> {
          if (held[0] != null) {
            held[0].close();
            held[0] = null;
          }
        }
        default -> {
        }
        }
      };
      final String forked = facetsJSON(sql);
      assertThat(squeezed.get()).as("forks counted with no budget left").isPositive();
      assertThat(forked).isEqualTo(sequential);
      assertThat(forked).doesNotContain(FacetResult.UNAVAILABLE);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(waitBefore);
    }
  }

  /**
   * Under PRECISION, a slice collector the budget refuses while it counts goes to a sketch instead of being refused. Absorbed, it would
   * sketch the facet, where the query thread alone (which waits for the budget) stays exact under the threshold: the slice is counted
   * again on the caller instead, so the answer is the sequential one and exact.
   */
  @Test
  @Timeout(300)
  void aSliceRefusedUnderPrecisionIsRecountedExactly() {
    load(30_000, 1);
    final Object waitBefore = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(200L);
    try {
      final String sql = "SELECT FROM " + TYPE + " FACETS n: fileSize DISTINCT PRECISION 100000000 LIMIT 5";
      parallel(1, 1);
      final FacetResults sequential = facets(sql);
      assertThat(sequential.exact()).as("sequential count under the threshold").isTrue();

      parallel(4, 1);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final FacetMemoryBudget.Reservation[] held = new FacetMemoryBudget.Reservation[1];
      final AtomicInteger squeezed = new AtomicInteger();
      FacetExecutor.parallelCountHook = event -> {
        switch (event) {
        // room left for the slices' sketches, not for their sets to grow: a refused slice goes to a sketch, not unavailable
        case FacetExecutor.FORK_SUBMITTED -> {
          held[0] = budget.reserve(budget.available() - 8 * DistinctFacetCollector.HLL_BYTES, 0);
          squeezed.incrementAndGet();
        }
        case FacetExecutor.FORK_COUNTED -> {
          if (held[0] != null) {
            held[0].close();
            held[0] = null;
          }
        }
        default -> {
        }
        }
      };
      final FacetResults forked = facets(sql);
      assertThat(squeezed.get()).as("forks counted with no budget left").isPositive();
      assertThat(forked.exact()).as("parallel count under the threshold").isTrue();
      assertThat(forked.toJSON().toString()).isEqualTo(sequential.toJSON().toString());
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(waitBefore);
    }
  }

  private FacetResults facets(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rs.next();
      return rs.getFacets();
    }
  }

  /**
   * A failure of the caller while the forked tasks still run (here a hook throwing once they are submitted; in production a terminated
   * statement, an interrupt or a failed task): when execute throws, every task has ended, and none counted a segment after the failure.
   */
  @Test
  @Timeout(300)
  void aFailedCallerEndsEveryForkedTaskFirst() {
    load(30_000, 1);
    parallel(4, 1);
    final CountDownLatch running = new CountDownLatch(1);
    FacetExecutor.parallelCountHook = event -> {
      switch (event) {
      case FacetExecutor.FORK_SUBMITTED -> {
        // a task is running before the caller fails (not merely queued, which abandon would cancel): bounded, a hang detector only
        try {
          running.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        throw new IllegalStateException("caller failed");
      }
      case FacetExecutor.TASK_START -> {
        started.incrementAndGet();
        running.countDown();
        try {
          // still running when the caller fails: it has to wait for this task
          Thread.sleep(200);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      case FacetExecutor.TASK_SEGMENT -> segments.incrementAndGet();
      case FacetExecutor.TASK_END -> ended.incrementAndGet();
      default -> {
      }
      }
    };
    final int[] observed = new int[2];
    try (final MatchSet matches = allMatches()) {
      assertThatThrownBy(() -> {
        try {
          FacetExecutor.execute((DatabaseInternal) database, matches, List.of(terms("brand")), new BasicCommandContext());
        } finally {
          observed[0] = started.get();
          observed[1] = ended.get();
        }
      }).isInstanceOf(IllegalStateException.class).hasMessage("caller failed");
    }
    assertThat(observed[0]).as("tasks forked").isPositive();
    assertThat(observed[1]).as("tasks ended when execute returned").isEqualTo(observed[0]);
    assertThat(segments.get()).as("segments counted by a task after the caller failed").isZero();
  }

  /**
   * Counting from the projection is not bound by the statement's deadline (a facet past it is still answered, final review A-M4), and
   * neither is parallel counting: past the deadline it answers what the query thread alone answers, its tasks forked and ended.
   */
  @Test
  @Timeout(300)
  void aPassedDeadlineCountsAsTheQueryThreadAlone() {
    load(30_000, 1);
    countTasks();
    final BasicCommandContext pastDeadline = new BasicCommandContext();
    pastDeadline.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline", false);
    try (final MatchSet matches = allMatches()) {
      parallel(1, 1);
      final String sequential = FacetExecutor.execute((DatabaseInternal) database, matches, List.of(terms("brand")), pastDeadline).toJSON()
          .toString();
      parallel(4, 1);
      final String forked = FacetExecutor.execute((DatabaseInternal) database, matches, List.of(terms("brand")), pastDeadline).toJSON()
          .toString();
      assertThat(forked).isEqualTo(sequential);
    }
    assertThat(started.get()).as("tasks forked").isPositive();
    assertThat(ended.get()).isEqualTo(started.get());
  }

  /** A facet with fewer matches in the bucket than parallelMinMatches is counted on the caller; with exactly as many it is forked. */
  @Test
  @Timeout(300)
  void belowMinMatchesStaysSequential() {
    load(30_000, 1);
    final String sql = "SELECT FROM " + TYPE + " WHERE fileSize > 1000 FACETS b: brand TERMS TOP 50 LIMIT 5";
    final long matches;
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE fileSize > 1000")) {
      matches = rs.next().<Number>getProperty("c").longValue();
    }
    countTasks();
    parallel(1, 1);
    final String sequential = facetsJSON(sql);

    parallel(4, matches + 1);
    assertThat(facetsJSON(sql)).isEqualTo(sequential);
    assertThat(started.get()).as("forked below parallelMinMatches").isZero();

    parallel(4, matches);
    assertThat(facetsJSON(sql)).isEqualTo(sequential);
    assertThat(started.get()).as("forked at parallelMinMatches").isPositive();
  }

  private MatchSet allMatches() {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      while (rs.hasNext())
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0)).isTrue();
      final MatchSet matches = builder.build();
      matches.setFacetFilterIndex(new int[] { -1 });
      return matches;
    }
  }

  private static FacetSpec terms(final String property) {
    return new TermsFacetSpec(property, property, null, null, 100, 1, FacetOrder.COUNT_DESC, null, false, null);
  }
}
