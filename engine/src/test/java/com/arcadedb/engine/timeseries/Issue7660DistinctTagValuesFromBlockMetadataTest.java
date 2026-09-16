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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7660: the distinct values of a TAG column are already written down, so reading every sample to recover
 * them is work nobody has to do.
 * <p>
 * Each sealed block's directory entry carries {@code tagDistinctValues} - the complete distinct value set of each
 * of its TAG columns, recorded at seal time from the very rows the block holds - and block-level tag pruning has
 * always relied on it. {@code TimeSeriesEngine.collectDistinctTagValues} unions those entries instead of walking
 * the samples, which turns a Grafana label picker from O(samples) into O(blocks x cardinality).
 * <p>
 * <b>The assertion that matters is not the speed, it is the EQUALITY.</b> Issue #7371 left this out precisely
 * because the two stores that hold the answer over-approximate in ways nobody had pinned down, and Prometheus
 * documents {@code /label/{name}/values} as the values a label actually carries: a stale extra value is a wrong
 * answer a user sees in a picker, not a free win. Every test below therefore compares against
 * {@link TimeSeriesEngine#forEachRow} over the same one-column projection - the scan this replaces, run in the
 * same JVM on the same data - rather than against a hand-written expected set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7660DistinctTagValuesFromBlockMetadataTest extends TestHelper {

  private static final long BASE_TS   = 1_700_000_000_000L;
  private static final long STEP_MS   = 10L;
  /**
   * Bucket-aligned compaction, so a few thousand samples seal into a few dozen blocks instead of one: the union
   * ACROSS block directory entries is the thing under test, and a single-block sealed layer would not exercise it.
   * A fixed-size chunk is {@code TimeSeriesShard.SEALED_BLOCK_SIZE} = 65536 samples, which is a slow way to make
   * the same point.
   */
  private static final long BUCKET_MS = 1_000L;

  /**
   * Both layers at once, over enough samples to make several sealed blocks: the union of the directory entries
   * plus the mutable scan is the set the full scan produces, value for value.
   */
  @Test
  void theSetIsTheOneTheFullScanProduces() throws Exception {
    final TimeSeriesEngine engine = createMetric("both_layers", 2);
    appendHosts(engine, 0, 2_000, 6);
    engine.compactAll();
    // Left uncompacted on purpose: a host that exists ONLY in the mutable bucket is how a sealed-only answer
    // gives itself away, since every assertion here would still pass on the six sealed names alone.
    appendHosts(engine, 1_000_000, 50, 1, "host_only_in_the_mutable_bucket");

    assertThat(engine.getShard(0).getSealedStore().getBlockCount())
        .as("the sealed layer has to hold more than one block for the union across blocks to be exercised")
        .isGreaterThan(1);

    assertThat(distinctTagValues(engine, "host"))
        .isEqualTo(scannedTagValues(engine, "host"))
        .contains("host_only_in_the_mutable_bucket");
  }

  /**
   * The point of the change: for a {@code STRING} TAG column no sealed block is decompressed at all. Every block
   * is counted as SKIPPED, no block is counted as read, and the only rows materialised are the mutable bucket's.
   */
  @Test
  void noSealedBlockIsDecompressedForAStringTag() throws Exception {
    final TimeSeriesEngine engine = createMetric("no_decompression", 1);
    appendHosts(engine, 0, 2_000, 4);
    engine.compactAll();

    final int sealedBlocks = engine.getShard(0).getSealedStore().getBlockCount();
    assertThat(sealedBlocks).isGreaterThan(1);

    final AggregationMetrics metrics = new AggregationMetrics();
    final Set<String> values = new LinkedHashSet<>();
    engine.collectDistinctTagValues("host", values, metrics);

    assertThat(values).hasSize(4);
    assertThat(metrics.getSkippedBlocks())
        .as("every sealed block answered from its directory entry, none of them read")
        .isEqualTo(sealedBlocks);
    assertThat(metrics.getSlowPathBlocks())
        .as("a block that had to be decompressed would be counted here")
        .isZero();
    assertThat(metrics.getMaterializedRows())
        .as("the mutable bucket is empty after compaction, so no row is materialised anywhere")
        .isZero();
  }

  /**
   * A tag value whose every sample has been dropped by retention must not be named. This is the staleness the
   * issue raised against {@code tagDistinctValues}: {@code TimeSeriesSealedStore.truncateBefore} drops WHOLE
   * blocks and copies every retained one verbatim, so a declaration never outlives the rows it was built from -
   * but a test that never expires anything would not notice if it did.
   */
  @Test
  void aValueRetentionDroppedIsNotNamed() throws Exception {
    final TimeSeriesEngine engine = createMetric("retained", 1);

    appendHosts(engine, 0, 500, 1, "expired_host");
    engine.compactAll();
    appendHosts(engine, 100_000, 500, 1, "surviving_host");
    engine.compactAll();

    assertThat(distinctTagValues(engine, "host"))
        .as("before retention both hosts are sealed and both are named")
        .contains("expired_host", "surviving_host");

    engine.applyRetention(BASE_TS + 50_000);

    assertThat(distinctTagValues(engine, "host"))
        .as("a host no surviving sample carries must not be named back to a label picker")
        .isEqualTo(scannedTagValues(engine, "host"))
        .doesNotContain("expired_host")
        .contains("surviving_host");
  }

  /**
   * Downsampling is the rewrite path that changes a block's rows rather than copying them, and it recomputes the
   * declaration from the rows it emits. It groups by tag key, so no tag value disappears and none is invented.
   */
  @Test
  void downsamplingKeepsTheDeclarationTrue() throws Exception {
    final TimeSeriesEngine engine = createMetric("downsampled", 1);
    appendHosts(engine, 0, 2_000, 3);
    engine.compactAll();

    final Set<String> before = distinctTagValues(engine, "host");

    // nowMs far past the newest sample so every block is older than the tier's cutoff, and a granularity coarse
    // enough against the 1ms sample spacing that the density heuristic selects the blocks rather than skipping them.
    engine.applyDownsampling(List.of(new DownsamplingTier(1_000L, 60_000L)), BASE_TS + 10_000_000L);

    assertThat(distinctTagValues(engine, "host"))
        .as("downsampling averages the fields per tag group; the tag values themselves survive unchanged")
        .isEqualTo(before)
        .isEqualTo(scannedTagValues(engine, "host"));
  }

  /**
   * A TAG column that is not a {@code STRING} is READ rather than trusted. The declaration stores
   * {@code value.toString()} while {@code decompressColumns} hands the scan
   * {@code ColumnDefinition.boxString(...)}, which is the identity only for {@code STRING} - so for any other type
   * the two can disagree and the block is decompressed instead.
   */
  @Test
  void aNonStringTagColumnIsReadRatherThanTrusted() throws Exception {
    new TimeSeriesTypeBuilder((DatabaseInternal) database)
        .withName("int_tag")
        .withTimestamp("ts")
        .withTag("code", Type.INTEGER)
        .withField("value", Type.DOUBLE)
        .withShards(1)
        .withCompactionBucketInterval(BUCKET_MS)
        .create();

    final TimeSeriesEngine engine = engineOf("int_tag");
    final int total = 2_000;
    final long[] timestamps = new long[total];
    final Object[] codes = new Object[total];
    final Object[] values = new Object[total];
    for (int i = 0; i < total; i++) {
      timestamps[i] = BASE_TS + i * STEP_MS;
      codes[i] = i % 3;
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { codes, values });
    engine.compactAll();

    final AggregationMetrics metrics = new AggregationMetrics();
    final Set<String> collected = new LinkedHashSet<>();
    engine.collectDistinctTagValues("code", collected, metrics);

    assertThat(collected)
        .as("the same values the scan produces, which is the only reason reading the block is acceptable here")
        .isEqualTo(scannedTagValues(engine, "code"))
        .containsExactlyInAnyOrder("0", "1", "2");
    assertThat(metrics.getSkippedBlocks())
        .as("no block may be answered from a declaration whose boxing the scan does not reproduce")
        .isZero();
    assertThat(metrics.getSlowPathBlocks())
        .isEqualTo(engine.getShard(0).getSealedStore().getBlockCount());
  }

  /**
   * A null tag value, spelled the way the scan spells it and not the way the metadata stores it.
   * <p>
   * The issue expected the two to disagree - block metadata storing a null tag as {@code ""} against a handler
   * that skips nulls - but no read path hands a null out for a {@code STRING} TAG at all. The mutable bucket
   * normalises it on the way out ({@code TimeSeriesBucket.readColumnValue} returns {@code ""} for a zero-length
   * STRING, and the tag dictionary maps {@code null} and {@code ""} onto the same {@code EMPTY_ID}), and
   * {@code compressColumn} writes it into a sealed block as {@code ""}. So {@code ""} is a label value here in
   * both layers, before and after this change alike, and the declaration agrees with the scan on it.
   */
  @Test
  void aNullTagValueIsSpelledTheWayTheScanSpellsIt() throws Exception {
    final TimeSeriesEngine engine = createMetric("null_tag", 1);
    engine.appendBatch(new long[] { BASE_TS, BASE_TS + 1, BASE_TS + 2 },
        new Object[][] { new Object[] { "h1", null, "h2" }, new Object[] { 1.0d, 2.0d, 3.0d } });

    assertThat(distinctTagValues(engine, "host"))
        .as("the mutable bucket hands a null tag back as the empty string, and so does the scan this replaces")
        .isEqualTo(scannedTagValues(engine, "host"))
        .containsExactlyInAnyOrder("h1", "h2", "");

    engine.compactAll();

    assertThat(distinctTagValues(engine, "host"))
        .as("once sealed the same null is the empty string in the declaration and in the scan alike")
        .isEqualTo(scannedTagValues(engine, "host"))
        .containsExactlyInAnyOrder("h1", "h2", "");
  }

  /**
   * Samples are routed round-robin, so a tag value can be absent from a shard entirely. The answer is the union
   * over every shard, not the first shard's.
   */
  @Test
  void everyShardContributes() throws Exception {
    final TimeSeriesEngine engine = createMetric("sharded", 4);
    appendHosts(engine, 0, 2_000, 7);
    engine.compactAll();

    assertThat(engine.getShardCount()).isEqualTo(4);
    assertThat(distinctTagValues(engine, "host"))
        .isEqualTo(scannedTagValues(engine, "host"))
        .hasSize(7);
  }

  /**
   * A type with nothing sealed yet still answers, from the mutable bucket alone - the branch where the block
   * directory is empty and the whole answer comes from the scan.
   */
  @Test
  void aTypeWithOnlyMutableRowsAnswersFromTheScan() throws Exception {
    final TimeSeriesEngine engine = createMetric("mutable_only", 1);
    appendHosts(engine, 0, 50, 3);

    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isZero();
    assertThat(distinctTagValues(engine, "host"))
        .isEqualTo(scannedTagValues(engine, "host"))
        .hasSize(3);
  }

  /**
   * The TIMESTAMP column is written at the head of every scanned row whatever its declared position, so the
   * projected slot of a TAG column is not its schema index. This layout - {@code [host, ts, value]} - makes the
   * two differ, and both the resolution of the projection and the indexing of the declaration have to survive it.
   */
  @Test
  void aTimestampDeclaredAfterATagDoesNotShiftTheAnswer() throws Exception {
    new TimeSeriesTypeBuilder((DatabaseInternal) database)
        .withName("mid_ts")
        .withTag("host", Type.STRING)
        .withTimestamp("ts")
        .withField("value", Type.DOUBLE)
        .withShards(1)
        .create();

    final TimeSeriesEngine engine = engineOf("mid_ts");
    engine.appendBatch(new long[] { BASE_TS, BASE_TS + 1 },
        new Object[][] { new Object[] { "h1", "h2" }, new Object[] { 1.0d, 2.0d } });
    engine.compactAll();

    assertThat(distinctTagValues(engine, "host"))
        .as("the host TAG's values, not the timestamps the row starts with")
        .isEqualTo(scannedTagValues(engine, "host"))
        .containsExactlyInAnyOrder("h1", "h2");
  }

  /** A FIELD is not a label, however well its name resolves against the schema, and neither is a name nobody declared. */
  @Test
  void aColumnThatIsNotADeclaredTagIsRejected() throws Exception {
    final TimeSeriesEngine engine = createMetric("not_a_tag", 1);
    appendHosts(engine, 0, 10, 2);

    assertThatThrownBy(() -> engine.collectDistinctTagValues("value", new LinkedHashSet<>(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("value");
    assertThatThrownBy(() -> engine.collectDistinctTagValues("nosuchcolumn", new LinkedHashSet<>(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nosuchcolumn");
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
    return engineOf(typeName);
  }

  private TimeSeriesEngine engineOf(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  /**
   * Appends {@code count} samples starting {@code startOffsetMs} after the base timestamp, {@link #STEP_MS} apart,
   * cycling over {@code hostCount} host names - or all carrying {@code fixedHost} when one is given.
   */
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

  /** The new reader's answer. */
  private static Set<String> distinctTagValues(final TimeSeriesEngine engine, final String tag) throws IOException {
    final Set<String> values = new LinkedHashSet<>();
    engine.collectDistinctTagValues(tag, values, null);
    return values;
  }

  /**
   * The answer the scan produces, written the way {@code GetPromQLLabelValuesHandler} wrote it before this change:
   * a one-column projection folded through {@code forEachRow}, taking the value from slot 1 - the projected row is
   * {@code { timestamp, the selected column }} - and skipping a null.
   */
  private static Set<String> scannedTagValues(final TimeSeriesEngine engine, final String tag) throws IOException {
    final int[] columnIndices = TimeSeriesGateway.resolveColumnIndices(List.of(tag),
        engine.getColumns());
    final Set<String> values = new LinkedHashSet<>();
    engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, columnIndices, null, null, row -> {
      if (row.length > 1 && row[1] != null)
        values.add(row[1].toString());
      return true;
    });
    return values;
  }
}
