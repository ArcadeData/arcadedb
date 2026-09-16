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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7709: the distinct values of a TAG column, bounded to the rows a time range actually holds.
 * <p>
 * Issue #7660 made {@code collectDistinctTagValues} answer the sealed layer from each block's declared value set
 * instead of scanning every sample, and left the RANGE out: the fold ran over the whole retention, so a picker
 * scoped to the last hour was offered every value the type had ever held. The bound has to be nearly free or it
 * gives back what #7660 bought, which is why it is applied per BLOCK: outside the range a block is dropped on its
 * directory entry, inside it the declaration still answers, and only the at most two blocks that straddle a bound
 * are decompressed and filtered per row.
 * <p>
 * Every ranged assertion below is compared against {@link TimeSeriesEngine#forEachRow} over the SAME range - the
 * scan the push-down replaces, run in the same JVM on the same data - for the reason #7660 gave: an
 * over-approximation on this endpoint is a wrong answer a user sees in a Grafana picker, and a hand-written
 * expected set would not catch one that came from the declaration rather than from the rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7709RangeScopedDistinctTagValuesTest extends TestHelper {

  private static final long BASE_TS   = 1_700_000_000_000L;
  private static final long STEP_MS   = 10L;
  /** Bucket-aligned compaction, so a few thousand samples seal into dozens of blocks rather than one. */
  private static final long BUCKET_MS = 1_000L;

  /**
   * The defect itself: a host whose every sample is older than the window must not be named by a request scoped to
   * that window, and must still be named by the unscoped one.
   */
  @Test
  void anOldHostIsNotNamedByARecentWindow() throws Exception {
    final TimeSeriesEngine engine = createMetric("windowed", 1);
    appendHosts(engine, 0, 500, 1, "old_host");
    appendHosts(engine, 100_000, 500, 1, "recent_host");
    engine.compactAll();

    assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
        .as("both hosts are there, so the ranged assertion is not asserting an absence that was never a presence")
        .containsExactlyInAnyOrder("old_host", "recent_host");

    assertThat(distinctTagValues(engine, "host", BASE_TS + 100_000, Long.MAX_VALUE))
        .isEqualTo(scannedTagValues(engine, "host", BASE_TS + 100_000, Long.MAX_VALUE))
        .containsExactly("recent_host");

    assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, BASE_TS + 4_990))
        .isEqualTo(scannedTagValues(engine, "host", Long.MIN_VALUE, BASE_TS + 4_990))
        .containsExactly("old_host");
  }

  /**
   * A bound that falls INSIDE a block is the one case the declaration cannot answer: the entry says which values
   * the block holds and nothing about which rows carry them. The value on the far side of the cut must be absent,
   * which is what says the straddling block was read rather than declared from.
   */
  @Test
  void aBoundInsideABlockIsAppliedPerRow() throws Exception {
    final TimeSeriesEngine engine = createMetric("straddle", 1);
    // One compaction bucket = 1s = 100 samples at STEP_MS. The first 50 carry "first_half", the rest "second_half",
    // so the two live in the SAME sealed block and only a per-row filter can separate them.
    appendHalves(engine);
    engine.compactAll();

    final long cut = BASE_TS + 50 * STEP_MS;

    assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
        .containsExactlyInAnyOrder("first_half", "second_half");

    assertThat(distinctTagValues(engine, "host", cut, Long.MAX_VALUE))
        .as("the cut is inside the block, so the block's own declaration would have named both")
        .isEqualTo(scannedTagValues(engine, "host", cut, Long.MAX_VALUE))
        .containsExactly("second_half");

    assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, cut - 1))
        .isEqualTo(scannedTagValues(engine, "host", Long.MIN_VALUE, cut - 1))
        .containsExactly("first_half");
  }

  /**
   * The cost of the bound: a block wholly inside the range is still answered from its declaration, and a block
   * outside it is not read either. Only the blocks holding a bound are decompressed, and there are at most two.
   */
  @Test
  void onlyTheBlocksHoldingABoundAreDecompressed() throws Exception {
    final TimeSeriesEngine engine = createMetric("cost", 1);
    appendHosts(engine, 0, 4_000, 4);
    engine.compactAll();

    final int sealedBlocks = engine.getShard(0).getSealedStore().getBlockCount();
    assertThat(sealedBlocks).as("several blocks, or there is no inside/outside to tell apart").isGreaterThan(4);

    final AggregationMetrics metrics = new AggregationMetrics();
    final Set<String> values = new LinkedHashSet<>();
    // Bounds deliberately off a bucket boundary, so each end lands inside a block rather than between two.
    engine.collectDistinctTagValues("host", BASE_TS + 5_005, BASE_TS + 25_005, values, metrics);

    assertThat(metrics.getSlowPathBlocks())
        .as("at most the two blocks that straddle a bound are read")
        .isLessThanOrEqualTo(2);
    assertThat(metrics.getSkippedBlocks() + metrics.getSlowPathBlocks())
        .as("every block is accounted for: dropped on its entry, answered from its declaration, or read")
        .isEqualTo(sealedBlocks);
  }

  /**
   * The mutable bucket takes the same bound. It carries no declaration and is scanned either way, but it must not
   * hand back a value from outside the window just because the scan is cheap.
   */
  @Test
  void theMutableBucketTakesTheBoundToo() throws Exception {
    final TimeSeriesEngine engine = createMetric("mutable", 1);
    appendHosts(engine, 0, 100, 1, "sealed_host");
    engine.compactAll();
    appendHosts(engine, 500_000, 100, 1, "mutable_old");
    appendHosts(engine, 900_000, 100, 1, "mutable_recent");

    assertThat(distinctTagValues(engine, "host", BASE_TS + 900_000, Long.MAX_VALUE))
        .isEqualTo(scannedTagValues(engine, "host", BASE_TS + 900_000, Long.MAX_VALUE))
        .containsExactly("mutable_recent");
  }

  /**
   * The compatibility half of the contract: an unscoped request answers exactly what it answered before the bounds
   * existed, which is what the 3-argument overload is for.
   */
  @Test
  void theUnscopedOverloadStillAnswersOverTheWholeSeries() throws Exception {
    final TimeSeriesEngine engine = createMetric("unscoped", 2);
    appendHosts(engine, 0, 2_000, 6);
    engine.compactAll();
    appendHosts(engine, 1_000_000, 50, 1, "host_only_in_the_mutable_bucket");

    final Set<String> unscoped = new LinkedHashSet<>();
    engine.collectDistinctTagValues("host", unscoped, null);

    assertThat(unscoped)
        .isEqualTo(distinctTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
        .isEqualTo(scannedTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
        .contains("host_only_in_the_mutable_bucket");
  }

  /** An empty range names nothing, and reads nothing to find that out. */
  @Test
  void aRangeHoldingNoSampleNamesNothing() throws Exception {
    final TimeSeriesEngine engine = createMetric("empty_window", 1);
    appendHosts(engine, 0, 500, 3);
    engine.compactAll();

    final AggregationMetrics metrics = new AggregationMetrics();
    final Set<String> values = new LinkedHashSet<>();
    engine.collectDistinctTagValues("host", BASE_TS + 10_000_000, BASE_TS + 20_000_000, values, metrics);

    assertThat(values).isEmpty();
    assertThat(metrics.getSlowPathBlocks()).as("no block is read to answer 'nothing here'").isZero();
  }

  /**
   * {@code hasRowsInRange} is the same bound for a metric NAME, which has no tag column and so no declaration to
   * read. It is what scopes {@code /label/__name__/values} to the window.
   */
  @Test
  void hasRowsInRangeAnswersWhetherTheWindowHoldsASample() throws Exception {
    final TimeSeriesEngine engine = createMetric("probe", 1);
    appendHosts(engine, 0, 500, 2);
    engine.compactAll();
    appendHosts(engine, 900_000, 10, 1, "mutable_only");

    assertThat(engine.hasRowsInRange(Long.MIN_VALUE, Long.MAX_VALUE, null)).isTrue();
    assertThat(engine.hasRowsInRange(BASE_TS, BASE_TS + 100, null)).as("sealed layer").isTrue();
    assertThat(engine.hasRowsInRange(BASE_TS + 900_000, Long.MAX_VALUE, null)).as("mutable bucket").isTrue();
    assertThat(engine.hasRowsInRange(BASE_TS + 10_000_000, BASE_TS + 20_000_000, null)).isFalse();

    final TimeSeriesEngine empty = createMetric("probe_empty", 1);
    assertThat(empty.hasRowsInRange(Long.MIN_VALUE, Long.MAX_VALUE, null))
        .as("a type holding no sample at all")
        .isFalse();
  }

  // --- helpers ---

  private TimeSeriesEngine createMetric(final String typeName, final int shards) {
    new TimeSeriesTypeBuilder((DatabaseInternal) database)
        .withName(typeName)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("value", Type.DOUBLE)
        .withShards(shards)
        .withCompactionBucketInterval(BUCKET_MS)
        .create();
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private void appendHosts(final TimeSeriesEngine engine, final long startOffsetMs, final int count,
      final int hostCount, final String... fixedHost) throws IOException {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
      hosts[i] = fixedHost.length > 0 ? fixedHost[0] : "host_" + (i % hostCount);
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /** 100 samples inside one compaction bucket, the first half under one host name and the second under another. */
  private void appendHalves(final TimeSeriesEngine engine) throws IOException {
    final long[] timestamps = new long[100];
    final Object[] hosts = new Object[100];
    final Object[] values = new Object[100];
    for (int i = 0; i < 100; i++) {
      timestamps[i] = BASE_TS + i * STEP_MS;
      hosts[i] = i < 50 ? "first_half" : "second_half";
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  private static Set<String> distinctTagValues(final TimeSeriesEngine engine, final String tag, final long fromTs,
      final long toTs) throws IOException {
    final Set<String> values = new LinkedHashSet<>();
    engine.collectDistinctTagValues(tag, fromTs, toTs, values, null);
    return values;
  }

  /** The answer the scan produces over the same range: the reference every ranged assertion is compared against. */
  private static Set<String> scannedTagValues(final TimeSeriesEngine engine, final String tag, final long fromTs,
      final long toTs) throws IOException {
    final int[] columnIndices = TimeSeriesGateway.resolveColumnIndices(List.of(tag), engine.getColumns());
    final Set<String> values = new LinkedHashSet<>();
    engine.forEachRow(fromTs, toTs, columnIndices, null, null, row -> {
      if (row.length > 1 && row[1] != null)
        values.add(row[1].toString());
      return true;
    });
    return values;
  }
}
