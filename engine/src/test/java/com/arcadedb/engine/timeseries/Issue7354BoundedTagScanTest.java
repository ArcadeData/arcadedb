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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7354: a reader whose ANSWER is O(label cardinality) had no way to ask for the rows without holding them.
 * <p>
 * {@code query()} merges every shard's full range into one {@code ArrayList} and sorts it by timestamp;
 * {@code iterateQuery()} skips the sort but the sealed layer still materialises every matching row before the
 * caller sees the first one, because the directory read lock has to be released first. So the PromQL label-values
 * and series endpoints - which read every row and keep a handful of strings - allocated the whole series per call.
 * <p>
 * {@code forEachRow} is the third shape: the same block walk, folding into the caller instead of into a list.
 * These tests pin the two things that makes it worth having - it sees exactly the rows the other two see, and its
 * residency is one block rather than the range.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7354BoundedTagScanTest extends TestHelper {

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
    // Left uncompacted on purpose, so the mutable layer is exercised alongside the sealed one: a scan that walked
    // only the sealed blocks would answer with a series that is missing its newest rows, and every assertion below
    // would still pass on a set of four host names.
    engine.appendBatch(new long[] { BASE_TS + PER_TAG * STEP_MS }, new Object[][] {
        new Object[] { "host_only_in_the_mutable_bucket" }, new Object[] { 1.0d } });
  }

  /** It sees the same rows the materialising readers see - the sealed layer and the mutable one alike. */
  @Test
  void everyRowTheMaterialisingReadersSeeReachesTheVisitor() throws Exception {
    final List<Object[]> reference = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);

    final List<Object[]> visited = new ArrayList<>();
    assertThat(engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> visited.add(row)))
        .as("a visitor that never says stop runs to the end")
        .isTrue();

    assertThat(visited).hasSameSizeAs(reference);
    assertThat(distinctHosts(visited))
        .as("and the rows are the same rows, whatever order they arrive in")
        .isEqualTo(distinctHosts(reference));
  }

  /** The time range is honoured exactly as the materialising readers honour it. */
  @Test
  void theRangeBoundsAreTheSameOnesTheOtherReadersApply() throws Exception {
    final long from = BASE_TS + 50 * STEP_MS;
    final long to = BASE_TS + 150 * STEP_MS;

    final List<Object[]> reference = engine.query(from, to, null, null);
    final List<Object[]> visited = new ArrayList<>();
    engine.forEachRow(from, to, null, null, null, row -> visited.add(row));

    assertThat(visited).hasSameSizeAs(reference);
    for (final Object[] row : visited)
      assertThat((long) row[0]).isBetween(from, to);
  }

  /** A tag filter pushes down the same way, so the visitor is not a filter-less back door onto the rows. */
  @Test
  void theTagFilterIsPushedDownAsItIsForTheOtherReaders() throws Exception {
    final TagFilter filter = TagFilter.eq(0, "host_2");

    final List<Object[]> reference = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, filter);
    final List<Object[]> visited = new ArrayList<>();
    engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, null, row -> visited.add(row));

    assertThat(visited).hasSameSizeAs(reference);
    assertThat(distinctHosts(visited)).containsExactly("host_2");
  }

  /**
   * The property the whole change is for: the scan is INCREMENTAL, so it can be stopped before it has read the
   * range. A materialising reader cannot be - by the time it returns, every row is already in memory - so a
   * visitor that stops on the first row and still causes one block to be decompressed is the observable
   * difference between the two, with no wall clock and no heap measurement involved.
   */
  @Test
  void aVisitorThatStopsEarlyStopsTheScan() throws Exception {
    int totalBlocks = 0;
    for (int i = 0; i < engine.getShardCount(); i++)
      totalBlocks += engine.getShard(i).getSealedStore().getBlockCount();
    assertThat(totalBlocks)
        .as("the fixture has to span several blocks, or 'only one was read' says nothing")
        .isGreaterThan(engine.getShardCount());

    final AggregationMetrics metrics = new AggregationMetrics();
    final AtomicInteger seen = new AtomicInteger();

    assertThat(engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics, row -> {
      seen.incrementAndGet();
      return false;
    })).as("a visitor that says stop is reported as having stopped it").isFalse();

    assertThat(seen.get()).isEqualTo(1);
    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("one block decompressed, not the range: the rows are produced as they are read")
        .isEqualTo(1);
    assertThat(metrics.getMaterializedRows())
        .as("and one row turned into an Object[], out of the whole series")
        .isEqualTo(1);
  }

  /** An empty range visits nothing and reads no block, rather than answering an empty list expensively. */
  @Test
  void aRangeWithNoRowsVisitsNothing() throws Exception {
    final AtomicInteger seen = new AtomicInteger();
    assertThat(engine.forEachRow(0L, 1L, null, null, null, row -> {
      seen.incrementAndGet();
      return true;
    })).isTrue();
    assertThat(seen.get()).isZero();
  }

  private static Set<String> distinctHosts(final List<Object[]> rows) {
    final Set<String> hosts = new LinkedHashSet<>();
    for (final Object[] row : rows)
      if (row[1] != null)
        hosts.add(row[1].toString());
    return hosts;
  }
}
