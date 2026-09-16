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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7336: {@code POST /ts/{database}/query} read a caller-supplied {@code limit} and then called
 * {@link TimeSeriesEngine#query}, which merges every shard's full range into one {@code ArrayList} and sorts
 * it, so ten rows over a wide range cost O(N) heap and O(N log N) time. {@link TimeSeriesEngine#iterateQuery}
 * is not a substitute - its own javadoc says the sealed layer materialises every matching row before the
 * iterator is returned - so the bound had to become a bound on the FETCH.
 * <p>
 * {@link TimeSeriesEngine#queryAscending} is the ascending mirror of {@code queryDescending} (issue #5414):
 * each shard stops walking blocks as soon as its own limit is satisfied.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7336AscendingLimitTest extends TestHelper {

  private static final int  TAGS    = 8;
  private static final int  PER_TAG = 20_000;
  private static final int  SHARDS  = 2;
  private static final long BASE_TS = 1_700_000_000_000L;
  private static final long STEP_MS = 1_000L;

  private TimeSeriesEngine engine;

  @BeforeEach
  void populate() throws IOException {
    database.command("sql",
        "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

    engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

    final int total = TAGS * PER_TAG;
    final long[] timestamps = new long[total];
    final Object[] hosts = new Object[total];
    final Object[] values = new Object[total];

    int i = 0;
    for (int t = 0; t < PER_TAG; t++)
      for (int h = 0; h < TAGS; h++) {
        timestamps[i] = BASE_TS + t * STEP_MS;
        hosts[i] = "host_" + h;
        values[i] = (double) (t * TAGS + h);
        i++;
      }

    engine.appendBatch(timestamps, new Object[][] { hosts, values });
    engine.compactAll();
  }

  /**
   * The defect itself: a small limit over the widest possible range must not touch the whole series.
   */
  @Test
  void ascendingScanTouchesOnlyTheOldestBlocks() throws Exception {
    int totalBlocks = 0;
    for (int i = 0; i < engine.getShardCount(); i++)
      totalBlocks += engine.getShard(i).getSealedStore().getBlockCount();
    assertThat(totalBlocks).isGreaterThan(engine.getShardCount());

    final TagFilter filter = TagFilter.eq(0, "host_3");

    // What the unbounded walk costs, measured rather than assumed: this is what engine.query() paid to answer
    // the same ten rows, and every bound below is stated against it so none of them can pass trivially.
    final AggregationMetrics unbounded = new AggregationMetrics();
    engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, unbounded, row -> true);
    final int unboundedBlocks = unbounded.getFastPathBlocks() + unbounded.getSlowPathBlocks();
    assertThat(unboundedBlocks).isGreaterThan(1);
    assertThat(unbounded.getMaterializedRows()).isEqualTo(PER_TAG);

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> oldest = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 10, metrics);

    assertThat(oldest).hasSize(10);
    assertThat((long) oldest.getFirst()[0]).isEqualTo(BASE_TS);

    // The whole point of the bound: at most the oldest block of each shard is decompressed.
    final int touched = metrics.getFastPathBlocks() + metrics.getSlowPathBlocks();
    assertThat(touched).isLessThanOrEqualTo(engine.getShardCount());
    assertThat(touched).isLessThan(totalBlocks);
    assertThat(touched).isLessThan(unboundedBlocks);

    // And the rows it boxed are bounded by the limit and the shard count, not by the series.
    assertThat(metrics.getMaterializedRows()).isLessThan(unbounded.getMaterializedRows());
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo((long) 10 * engine.getShardCount());
  }

  /**
   * The bounded fetch must answer exactly what {@code query()} answered, truncated: same rows, same order.
   */
  @Test
  void ascendingScanReturnsTheOldestRowsInOrder() throws Exception {
    final TagFilter filter = TagFilter.eq(0, "host_5");
    final List<Object[]> rows = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 100, null);

    assertThat(rows).hasSize(100);
    for (int i = 1; i < rows.size(); i++)
      assertThat((long) rows.get(i)[0]).isGreaterThan((long) rows.get(i - 1)[0]);

    final List<Object[]> reference = ascending(Long.MIN_VALUE, Long.MAX_VALUE, filter);
    for (int i = 0; i < rows.size(); i++) {
      assertThat((long) rows.get(i)[0]).isEqualTo((long) reference.get(i)[0]);
      assertThat(((Number) rows.get(i)[2]).doubleValue()).isEqualTo(((Number) reference.get(i)[2]).doubleValue());
    }
  }

  /**
   * A limit of zero or less is unlimited, exactly as on {@code queryDescending}, and the time bounds still apply.
   */
  @Test
  void ascendingScanHonoursTimeBoundsAndUnlimitedMode() throws Exception {
    final TagFilter filter = TagFilter.eq(0, "host_1");
    final long from = BASE_TS + 50 * STEP_MS;
    final long to = BASE_TS + 150 * STEP_MS;

    final List<Object[]> reference = ascending(from, to, filter);
    assertThat(reference).hasSize(101);

    for (final int unlimited : new int[] { 0, -1 }) {
      final List<Object[]> rows = engine.queryAscending(from, to, null, filter, unlimited, null);
      assertThat(rows).hasSameSizeAs(reference);
      for (int i = 0; i < rows.size(); i++)
        assertThat((long) rows.get(i)[0]).isEqualTo((long) reference.get(i)[0]);
    }

    // A limit larger than the range holds returns the range, not a padded answer.
    final List<Object[]> overshoot = engine.queryAscending(from, to, null, filter, 1_000, null);
    assertThat(overshoot).hasSameSizeAs(reference);

    // And a limit of exactly the range's size is not reported any differently.
    final List<Object[]> exact = engine.queryAscending(from, to, null, filter, reference.size(), null);
    assertThat(exact).hasSameSizeAs(reference);
    assertThat((long) exact.getLast()[0]).isEqualTo(to);
  }

  /**
   * The mutable layer has to be asked for the WHOLE limit, not for the remainder the sealed layer left
   * unfilled. Late arrivals carry OLD timestamps, so an answer can be made entirely of mutable rows even when
   * the sealed layer already produced rows of its own - which is why {@code TimeSeriesShard.scanRangeAscending}
   * passes {@code limit} down rather than {@code need - sealedRows.size()}.
   * <p>
   * One shard on purpose: the parameter under test lives in the shard, and round-robin routing would otherwise
   * split the late arrivals across shards and let the engine's merge paper over a wrong per-shard bound.
   */
  @Test
  void ascendingScanAsksTheMutableLayerForTheWholeLimitNotTheRemainder() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Late TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine late = ((LocalTimeSeriesType) database.getSchema().getType("Late")).getEngine();

    final int sealed = 10;
    final long[] sealedTs = new long[sealed];
    final Object[] sealedHosts = new Object[sealed];
    final Object[] sealedValues = new Object[sealed];
    for (int i = 0; i < sealed; i++) {
      sealedTs[i] = BASE_TS + i * STEP_MS;
      sealedHosts[i] = "host_0";
      sealedValues[i] = (double) i;
    }
    late.appendBatch(sealedTs, new Object[][] { sealedHosts, sealedValues });
    late.compactAll();

    // Six late arrivals, all OLDER than anything sealed, still in the mutable layer.
    final int lateCount = 6;
    final long[] lateTs = new long[lateCount];
    final Object[] lateHosts = new Object[lateCount];
    final Object[] lateValues = new Object[lateCount];
    for (int i = 0; i < lateCount; i++) {
      lateTs[i] = BASE_TS - (lateCount - i) * STEP_MS;
      lateHosts[i] = "host_0";
      lateValues[i] = (double) -(lateCount - i);
    }
    late.appendBatch(lateTs, new Object[][] { lateHosts, lateValues });

    // The range is narrowed so the sealed layer contributes exactly two rows, leaving a remainder of three.
    // Asking the mutable layer for that remainder returns only the three oldest late arrivals and then pads the
    // answer with the two sealed rows - the wrong set, because the five oldest are all late arrivals.
    final List<Object[]> rows = late.queryAscending(Long.MIN_VALUE, BASE_TS + STEP_MS, null, null, 5, null);

    assertThat(rows).hasSize(5);
    for (int i = 0; i < 5; i++) {
      assertThat((long) rows.get(i)[0]).isEqualTo(BASE_TS - (lateCount - i) * STEP_MS);
      assertThat(((Number) rows.get(i)[2]).doubleValue()).isEqualTo(-(double) (lateCount - i));
    }
  }

  /**
   * The simpler shape of the same hazard, through the sharded fixture: a late arrival older than everything
   * sealed must still lead the ascending answer.
   */
  @Test
  void ascendingScanSeesAnOlderRowStillInTheMutableLayer() throws Exception {
    final long olderTs = BASE_TS - 10 * STEP_MS;
    engine.appendSamples(new long[] { olderTs }, new Object[] { "host_3" }, new Object[] { -1.0 });

    final TagFilter filter = TagFilter.eq(0, "host_3");
    final List<Object[]> rows = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 2, null);

    assertThat(rows).hasSize(2);
    assertThat((long) rows.getFirst()[0]).isEqualTo(olderTs);
    assertThat(((Number) rows.getFirst()[2]).doubleValue()).isEqualTo(-1.0);
    assertThat((long) rows.get(1)[0]).isEqualTo(BASE_TS);
  }

  /**
   * The projection and the tag filter must survive the bounded path: a subset of columns still resolves the
   * filter against the right column, which is the trap {@code matchesMapped} exists for.
   */
  @Test
  void ascendingScanAppliesTheTagFilterUnderAColumnProjection() throws Exception {
    final TagFilter filter = TagFilter.eq(0, "host_7");
    final int[] projection = new int[] { 0 };

    final List<Object[]> rows = engine.queryAscending(BASE_TS, BASE_TS + 5 * STEP_MS, projection, filter, 3, null);

    assertThat(rows).hasSize(3);
    for (final Object[] row : rows) {
      assertThat(row).hasSize(2);
      assertThat(row[1]).isEqualTo("host_7");
    }
    assertThat((long) rows.getFirst()[0]).isEqualTo(BASE_TS);
    assertThat((long) rows.getLast()[0]).isEqualTo(BASE_TS + 2 * STEP_MS);
  }

  /**
   * Rows are routed to shards round-robin, so a filter matching nothing must still terminate, and a query
   * whose range holds fewer rows than the limit must not invent any.
   */
  @Test
  void ascendingScanOnAnEmptySelectionReturnsNothing() throws Exception {
    assertThat(engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "absent"), 10, null))
        .isEmpty();
    assertThat(engine.queryAscending(BASE_TS - 100_000L, BASE_TS - 1L, null, null, 10, null)).isEmpty();
  }

  /**
   * Across shards the merge has to be a real merge: the oldest N rows of the type span every shard, since a
   * timestamp is shared by all tags and the tags are spread round-robin.
   */
  @Test
  void ascendingScanMergesAcrossShards() throws Exception {
    final List<Object[]> rows = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, null, TAGS, null);

    assertThat(rows).hasSize(TAGS);
    final List<Object> hosts = new ArrayList<>();
    for (final Object[] row : rows) {
      assertThat((long) row[0]).isEqualTo(BASE_TS);
      hosts.add(row[1]);
    }
    // Every tag shares the oldest timestamp, so the merge must have visited both shards to find them all.
    assertThat(hosts).hasSize(TAGS).doesNotHaveDuplicates();
  }

  private List<Object[]> ascending(final long fromTs, final long toTs, final TagFilter filter) throws IOException {
    final List<Object[]> all = new ArrayList<>();
    final Iterator<Object[]> it = engine.iterateQuery(fromTs, toTs, null, filter);
    while (it.hasNext())
      all.add(it.next());
    return all;
  }
}
