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
 * Issue #9420: {@code iterateQuery} returned an {@code Iterator}, but every matching sealed row of every shard was
 * already in memory when it returned, so a plain {@code SELECT FROM <timeseries type>} with no LIMIT - and the gRPC
 * stream without a row limit - needed heap for the whole range before handing back the first row. A 50M-point type
 * exhausted a 4 GB heap that way (#9416).
 * <p>
 * The sealed layer is now read one block per shard at a time, over the same directory snapshot and block walk
 * {@code forEachRow} uses. These tests pin what that must keep - the same rows, in timestamp order - and what it
 * changes: the blocks are decoded as the iterator advances, not up front, which is observable through the read
 * counters with no heap measurement and no wall clock involved.
 */
class Issue9420LazyIterateQueryTest extends TestHelper {

  private static final int  TAGS    = 8;
  private static final int  PER_TAG = 50_000;
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
    // Left in the mutable bucket on purpose: a lazy sealed walk that lost the mutable layer would still pass every
    // ordering assertion below.
    engine.appendBatch(new long[] { BASE_TS + PER_TAG * STEP_MS }, new Object[][] {
        new Object[] { "host_only_in_the_mutable_bucket" }, new Object[] { 1.0d } });
  }

  /** The same rows the materialising {@code query()} returns, sealed and mutable alike, in timestamp order. */
  @Test
  void returnsEveryRowOfBothLayersInTimestampOrder() throws Exception {
    final List<Object[]> reference = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    final List<Object[]> iterated = drain(engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null));

    assertThat(iterated).hasSameSizeAs(reference).hasSize(TAGS * PER_TAG + 1);
    long previous = Long.MIN_VALUE;
    for (final Object[] row : iterated) {
      assertThat((long) row[0]).as("the shards are still merged by timestamp").isGreaterThanOrEqualTo(previous);
      previous = (long) row[0];
    }
    assertThat(iterated.getLast()[1]).isEqualTo("host_only_in_the_mutable_bucket");
  }

  /** Range bounds and tag filter apply exactly as they do for {@code query()}. */
  @Test
  void rangeAndTagFilterMatchTheMaterialisingReader() throws Exception {
    final long from = BASE_TS + 500 * STEP_MS;
    final long to = BASE_TS + 15_000 * STEP_MS;
    final TagFilter filter = TagFilter.eq(0, "host_3");

    final List<Object[]> reference = engine.query(from, to, null, filter);
    final List<Object[]> iterated = drain(engine.iterateQuery(from, to, null, filter));

    assertThat(iterated).hasSameSizeAs(reference).isNotEmpty();
    for (final Object[] row : iterated) {
      assertThat((long) row[0]).isBetween(from, to);
      assertThat(row[1]).isEqualTo("host_3");
    }
  }

  /**
   * The property the issue is about. Before the fix the call itself decoded every block of the range and built
   * every row; now it decodes at most one block per shard to find each shard's first row, and the rest are read
   * as the iterator reaches them.
   */
  @Test
  void blocksAreDecodedAsTheIteratorAdvancesNotUpFront() throws Exception {
    final int totalBlocks = totalSealedBlocks();
    assertThat(totalBlocks)
        .as("the fixture has to span several blocks per shard, or 'one per shard' says nothing")
        .isGreaterThanOrEqualTo(3 * SHARDS);

    final AggregationMetrics metrics = new AggregationMetrics();
    final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics);
    assertThat(it.hasNext()).isTrue();
    it.next();

    assertThat(decodedBlocks(metrics))
        .as("one block per shard is enough to know each shard's first row")
        .isLessThanOrEqualTo(SHARDS);
    assertThat(metrics.getMaterializedRows())
        .as("and only those blocks' rows were built, out of %d", TAGS * PER_TAG)
        .isLessThan(TAGS * PER_TAG / 2);

    while (it.hasNext())
      it.next();
    assertThat(decodedBlocks(metrics)).as("draining it still reads every block once").isEqualTo(totalBlocks);
  }

  /**
   * The read now holds nothing of the shard between two blocks, like {@code forEachRow}, so a retention pass can
   * land mid-iteration. It must leave a correct, shorter answer - the truncated rows are gone - and say so through
   * {@code vanishedBlocks}, never fail the read or hand back a row older than the cutoff it has not yet passed.
   */
  @Test
  void aRetentionPassMidIterationSkipsTheBlocksItRemoved() throws Exception {
    final AggregationMetrics metrics = new AggregationMetrics();
    final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics);
    final long firstTs = (long) it.next()[0];

    final long cutoff = BASE_TS + (PER_TAG / 2) * STEP_MS;
    engine.applyRetention(cutoff);

    final List<Object[]> rest = drain(it);
    assertThat(metrics.getVanishedBlocks()).as("the blocks retention removed are counted, not hidden").isPositive();
    assertThat(rest.size() + 1).as("the answer is short by the rows retention removed").isLessThan(TAGS * PER_TAG + 1);
    assertThat(rest.getLast()[1]).isEqualTo("host_only_in_the_mutable_bucket");
    assertThat(firstTs).isEqualTo(BASE_TS);
  }

  private int totalSealedBlocks() {
    int total = 0;
    for (int i = 0; i < engine.getShardCount(); i++)
      total += engine.getShard(i).getSealedStore().getBlockCount();
    return total;
  }

  private static long decodedBlocks(final AggregationMetrics metrics) {
    return metrics.getFastPathBlocks() + metrics.getSlowPathBlocks();
  }

  private static List<Object[]> drain(final Iterator<Object[]> it) {
    final List<Object[]> rows = new ArrayList<>();
    while (it.hasNext())
      rows.add(it.next());
    return rows;
  }
}
