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

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5416: the newest-first scan added by #5414 only covered the sealed layer, so an unbounded
 * last-point query still read the whole unsealed tail: {@code TimeSeriesBucket.scanRange()} walks
 * data pages front to back and materialises every matching row, ignoring the limit. The mutable
 * bucket now has its own descending walk with page pruning and early exit, the sealed layer boxes
 * only the rows it returns, and EXPLAIN shows the pushed-down tag filter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5416DescendingTailTest extends TestHelper {

  private static final int  TAGS    = 4;
  private static final long BASE_TS = 1_700_000_000_000L;
  private static final long STEP_MS = 1_000L;

  /**
   * Creates the type and appends {@code ticks} samples per tag, all of them left unsealed.
   */
  private TimeSeriesEngine createUnsealed(final int ticks, final int shards) throws IOException {
    database.command("sql",
        "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + shards);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();
    append(engine, 0, ticks);
    return engine;
  }

  private void append(final TimeSeriesEngine engine, final int fromTick, final int toTick) throws IOException {
    final int rows = (toTick - fromTick) * TAGS;
    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] values = new Object[rows];

    int i = 0;
    for (int t = fromTick; t < toTick; t++)
      for (int h = 0; h < TAGS; h++) {
        timestamps[i] = BASE_TS + (long) t * STEP_MS;
        hosts[i] = "host_" + h;
        values[i] = (double) (t * TAGS + h);
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /**
   * Rows are {@code Object[]}, whose {@code equals} is identity: compare their contents instead.
   */
  private static List<List<Object>> deep(final List<Object[]> rows) {
    final List<List<Object>> out = new ArrayList<>(rows.size());
    for (final Object[] row : rows)
      out.add(Arrays.asList(row));
    return out;
  }

  private static List<Long> timestamps(final List<Object[]> rows) {
    final List<Long> out = new ArrayList<>(rows.size());
    for (final Object[] row : rows)
      out.add((Long) row[0]);
    return out;
  }

  /**
   * Reference implementation: full ascending scan, filtered and sorted in memory.
   */
  private List<Object[]> expectedNewest(final TimeSeriesEngine engine, final long fromTs, final long toTs,
      final String host, final int limit) throws IOException {
    final List<Object[]> all = new ArrayList<>(
        engine.query(fromTs, toTs, null, host == null ? null : TagFilter.eq(0, host)));
    all.sort((a, b) -> Long.compare((long) b[0], (long) a[0]));
    return limit > 0 && all.size() > limit ? new ArrayList<>(all.subList(0, limit)) : all;
  }

  @Test
  void unsealedTailAnswersTheNewestPointWithoutMaterialisingIt() throws IOException {
    // A single shard so the tag is guaranteed to be present in it: with several shards, a shard
    // holding no matching row has nothing to build a cut-off from and has to look at every page.
    final TimeSeriesEngine engine = createUnsealed(20_000, 1);

    // Everything is still in the mutable layer: no block can be involved.
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isZero();

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_2"), 1, metrics);

    assertThat(deep(newest)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_2", 1)));

    // The point of the fix: the 80k unsealed rows are no longer turned into Object[]. Only the newest
    // page is examined, and every older page is dropped on its header alone.
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(4);
    assertThat(metrics.getScannedPages()).isLessThanOrEqualTo(2);
    assertThat(metrics.getSkippedPages()).isGreaterThan(100);
  }

  /**
   * Samples are routed round-robin, so a tag can be missing from a shard entirely. Such a shard has
   * no matching row to build a cut-off from, and only the bound carried over from the shards already
   * visited stops it from reading everything it owns.
   */
  @Test
  void aShardWithoutTheTagIsPrunedByTheRunningBound() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(20_000, 4);

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_1"), 1, metrics);

    assertThat(deep(newest)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", 1)));

    // Only the shards visited before the first one holding the tag can be forced to walk their pages.
    final int totalPages = metrics.getScannedPages() + metrics.getSkippedPages();
    assertThat(metrics.getScannedPages()).isLessThan(totalPages * 3 / 4);
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(4);
  }

  @Test
  void unsealedTailTopNMatchesAFullScan() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(5_000, 2);

    for (final int limit : new int[] { 1, 2, 7, 100, 5_000, 10_000 })
      assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), limit,
          null)))
          .as("limit " + limit)
          .isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", limit)));
  }

  @Test
  void unsealedTailWithoutTagFilterMatchesAFullScan() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(2_000, 2);

    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, null, 10, null);

    // All tags share the same timestamps here, so which row wins a tie is arbitrary in both
    // directions: compare the timestamp sequence, which is what the order is defined on.
    assertThat(timestamps(rows)).isEqualTo(timestamps(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, null, 10)));
    assertThat(deep(rows)).isSubsetOf(deep(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)));
  }

  @Test
  void unsealedTailHonoursTimeBounds() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(3_000, 2);

    final long fromTs = BASE_TS + 1_000 * STEP_MS;
    final long toTs = BASE_TS + 1_500 * STEP_MS;
    final List<Object[]> rows = engine.queryDescending(fromTs, toTs, null, TagFilter.eq(0, "host_0"), 5, null);

    assertThat(deep(rows)).isEqualTo(deep(expectedNewest(engine, fromTs, toTs, "host_0", 5)));
    assertThat((long) rows.getFirst()[0]).isEqualTo(toTs);
  }

  /**
   * Late arrivals are appended to the last page, so pages are not globally ordered by timestamp. The
   * descending walk must skip a page it cannot use rather than stop at it.
   */
  @Test
  void outOfOrderArrivalsAreStillReturnedNewestFirst() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(4_000, 1);

    // A batch older than everything already stored, landing on the newest pages.
    final long[] timestamps = new long[] { BASE_TS - 5_000, BASE_TS - 4_000 };
    engine.appendBatch(timestamps,
        new Object[][] { new Object[] { "host_0", "host_0" }, new Object[] { -1.0, -2.0 } });

    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_0"),
        10, null);
    assertThat(deep(rows)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_0", 10)));

    // And the old rows are reachable when the range asks for them.
    final List<Object[]> old = engine.queryDescending(Long.MIN_VALUE, BASE_TS - 1, null, TagFilter.eq(0, "host_0"), 10,
        null);
    assertThat(old).hasSize(2);
    assertThat((long) old.getFirst()[0]).isEqualTo(BASE_TS - 4_000);
  }

  @Test
  void sealedAndUnsealedLayersAreMergedNewestFirst() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(5_000, 2);
    engine.compactAll();
    append(engine, 5_000, 5_400);

    long mutableRows = 0;
    int blocks = 0;
    for (int s = 0; s < engine.getShardCount(); s++) {
      mutableRows += engine.getShard(s).getMutableBucket().getSampleCount();
      blocks += engine.getShard(s).getSealedStore().getBlockCount();
    }
    assertThat(mutableRows).isPositive();
    assertThat(blocks).isPositive();

    // A limit that straddles the boundary has to pull from both layers.
    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_3"),
        600, null);
    assertThat(deep(rows)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_3", 600)));
  }

  /**
   * The page-level filter compares raw UTF-8 bytes for STRING columns; any other type still has to
   * go through the decoded value, otherwise the leading bytes would be read as a length prefix.
   */
  @Test
  void aFilterOnANonStringColumnStillMatches() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(500, 1);

    // Column 1 is the DOUBLE value, not the tag.
    final double value = 4.0 * 100 + 2;
    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(1, value), 5,
        null);

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[2]).isEqualTo(value);
    assertThat(rows.getFirst()[1]).isEqualTo("host_2");
  }

  @Test
  void columnSubsetKeepsTheTagFilterSemantics() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(500, 1);
    final TimeSeriesShard shard = engine.getShard(0);

    // Column 0 is the tag: asking for it keeps the filter usable.
    final List<Object[]> withTag = shard.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 },
        TagFilter.eq(0, "host_1"), 3, null);
    assertThat(withTag).hasSize(3);
    for (final Object[] row : withTag)
      assertThat(row[1]).isEqualTo("host_1");

    // Asking only for the value column drops it, and a filter on a column that is not returned can
    // never match. Same contract as the ascending scan.
    final List<Object[]> withoutTag = shard.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 1 },
        TagFilter.eq(0, "host_1"), 3, null);
    final List<Object[]> ascendingWithoutTag = shard.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 1 },
        TagFilter.eq(0, "host_1"));
    assertThat(withoutTag).isEmpty();
    assertThat(ascendingWithoutTag).isEmpty();
  }

  @Test
  void emptyBucketReturnsNothing() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_0"), 1, null))
        .isEmpty();
  }

  /**
   * The sealed layer must return exactly what the boxed implementation used to return, including the
   * INTEGER / LONG / STRING value types.
   */
  @Test
  void sealedRowsKeepTheirValueTypes() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Mixed TIMESTAMP ts TAGS (host STRING) "
        + "FIELDS (d DOUBLE, i INTEGER, l LONG)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Mixed")).getEngine();

    final int rows = 1_000;
    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] doubles = new Object[rows];
    final Object[] ints = new Object[rows];
    final Object[] longs = new Object[rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = BASE_TS + (long) i * STEP_MS;
      hosts[i] = "host_" + (i % 2);
      doubles[i] = i + 0.5d;
      ints[i] = i;
      longs[i] = (long) i * 1_000_000L;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, doubles, ints, longs });
    engine.compactAll();

    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_1"), 1, null);

    assertThat(newest).hasSize(1);
    final Object[] row = newest.getFirst();
    assertThat(row[0]).isEqualTo(BASE_TS + 999L * STEP_MS);
    assertThat(row[1]).isEqualTo("host_1");
    assertThat(row[2]).isEqualTo(999.5d);
    assertThat(row[3]).isEqualTo(999);
    assertThat(row[4]).isEqualTo(999_000_000L);

    // And it agrees with the ascending scan, which still uses the boxed path.
    assertThat(deep(newest)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", 1)));
  }

  @Test
  void sealedTopOneBoxesOnlyTheRowsItReturns() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(20_000, 2);
    engine.compactAll();

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_2"), 1, metrics);

    assertThat(newest).hasSize(1);
    // One row per shard at most, instead of one boxed value per row stored in the block.
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(engine.getShardCount());
  }

  @Test
  void explainShowsThePushedDownTagFilter() throws IOException {
    createUnsealed(100, 1);

    final String plan = explain("SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts DESC LIMIT 1");
    assertThat(plan).contains("FETCH FROM TIMESERIES DESC Point");
    assertThat(plan).contains("TAGS host = 'host_1'");
    assertThat(plan).contains("TOP 1");
  }

  @Test
  void explainShowsTheTagFilterOnTheAscendingFetchToo() throws IOException {
    createUnsealed(100, 1);

    final String plan = explain("SELECT ts, value FROM Point WHERE host = 'host_1'");
    assertThat(plan).contains("FETCH FROM TIMESERIES Point");
    assertThat(plan).contains("TAGS host = 'host_1'");
    assertThat(plan).doesNotContain("TOP ");
  }

  @Test
  void explainShowsNoTagsWhenThereIsNoTagPredicate() throws IOException {
    createUnsealed(100, 1);

    assertThat(explain("SELECT ts, value FROM Point ORDER BY ts DESC LIMIT 1")).doesNotContain("TAGS");
  }

  /**
   * The planner only turns tag equalities into a {@link TagFilter} today, but the renderer has to
   * cover the multi-value and multi-condition forms the engine API can build.
   */
  @Test
  void tagFilterIsRenderedForEveryConditionShape() {
    final String[] names = new String[] { "host", "rack" };

    assertThat(TagFilter.eq(0, "web_1").describe(names)).isEqualTo("host = 'web_1'");
    assertThat(TagFilter.eq(0, "web_1").and(1, 7).describe(names)).isEqualTo("host = 'web_1' AND rack = 7");
    assertThat(TagFilter.in(0, new LinkedHashSet<>(List.of("b", "a"))).describe(names))
        .isEqualTo("host IN ['a', 'b']");

    // Unknown or missing names fall back to the positional form.
    assertThat(TagFilter.eq(3, "x").describe(names)).isEqualTo("col3 = 'x'");
    assertThat(TagFilter.eq(0, "x").describe(null)).isEqualTo("col0 = 'x'");
  }

  @Test
  void sqlLastPointOnAnUnsealedTailReturnsTheNewestRow() throws IOException {
    final TimeSeriesEngine engine = createUnsealed(10_000, 2);

    final long expectedTs = (long) expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", 1).getFirst()[0];

    try (final ResultSet rs = database.query("sql",
        "SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts DESC LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((LocalDateTime) row.getProperty("ts")).toInstant(ZoneOffset.UTC).toEpochMilli())
          .isEqualTo(expectedTs);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return (String) rs.next().getProperty("executionPlanAsString");
    }
  }
}
