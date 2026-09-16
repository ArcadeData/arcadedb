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
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7710: {@code GET /prom/api/v1/series} answers the distinct label COMBINATIONS a metric carries, and used
 * to walk every sample of every matching type to find them - on the same Grafana dashboard-load path #7660
 * fixed for label-values.
 * <p>
 * Nothing on disk records a tuple: {@code BlockEntry.tagDistinctValues} is per COLUMN, so the cross product of
 * two columns' declared sets OVER-COUNTS - a block declaring {@code host in {h1, h2}} and
 * {@code region in {eu, us}} is consistent with two combinations and with four, and the entry cannot say which.
 * What a block CAN answer alone is the case where it declares every tag column as a single value: that block
 * holds exactly one combination, which is the common shape for Prometheus data, where one block holds one series.
 * <p>
 * What is pinned here is that the fold over {@code forEachTagCombination} reaches the same answer as the fold
 * over {@code forEachRow} - the set of combinations, and the earliest timestamp each was observed at - and that
 * it reaches it without decompressing the blocks it could answer from their directory entries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7710TagCombinationsFromBlockMetadataTest extends TestHelper {

  private static final int[] TAG_PROJECTION = { 0, 1 }; // host, region: the non-timestamp indices of the tags

  private static List<ColumnDefinition> twoTagColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("region", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  /**
   * The shape the optimisation is for: each series sealed into its own block, so every block declares one value
   * per tag column and the whole answer comes off the directory.
   */
  @Test
  void answersABlockHoldingOneSeriesFromItsDirectoryEntry() throws Exception {
    final TimeSeriesEngine engine = engineFor("ts_series_per_block");
    try {
      appendAndSeal(engine, "h1", "eu", 1000L, 5);
      appendAndSeal(engine, "h2", "us", 2000L, 5);

      final AggregationMetrics metrics = new AggregationMetrics();
      final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE, metrics);

      assertThat(combinations).containsOnlyKeys("h1|eu", "h2|us");
      assertThat(combinations.get("h1|eu")).isEqualTo(1000L);
      assertThat(combinations.get("h2|us")).isEqualTo(2000L);

      assertThat(metrics.getMaterializedRows())
          .as("not one sample is read: both blocks are answered from their directory entries").isZero();
      assertThat(metrics.getSkippedBlocks()).isEqualTo(2);

      assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
    } finally {
      engine.close();
    }
  }

  /**
   * The case the issue says a test has to hold: ONE block carrying two combinations. Its per-column declaration
   * is {@code host in {h1, h2}} and {@code region in {eu, us}}, whose cross product is four - so a reader that
   * trusted it would report two series that do not exist. The block must be read.
   */
  @Test
  void readsABlockHoldingMoreThanOneCombinationRatherThanCrossingItsColumns() throws Exception {
    final TimeSeriesEngine engine = engineFor("ts_two_series_one_block");
    try {
      // Interleaved, so both series live in the same sealed block.
      final int count = 10;
      final long[] timestamps = new long[count];
      final Object[] hosts = new Object[count];
      final Object[] regions = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = 1000L + i * 1000L;
        hosts[i] = i % 2 == 0 ? "h1" : "h2";
        regions[i] = i % 2 == 0 ? "eu" : "us";
        values[i] = (double) i;
      }
      append(engine, timestamps, hosts, regions, values);
      seal(engine);
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      final AggregationMetrics metrics = new AggregationMetrics();
      final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE, metrics);

      assertThat(combinations)
          .as("two combinations, not the four the cross product of the declared sets would give")
          .containsOnlyKeys("h1|eu", "h2|us");
      assertThat(combinations.get("h1|eu")).isEqualTo(1000L);
      assertThat(combinations.get("h2|us")).isEqualTo(2000L);

      assertThat(metrics.getMaterializedRows())
          .as("a block that cannot answer from its entry is READ, exactly as forEachRow would read it")
          .isEqualTo(count);

      assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
    } finally {
      engine.close();
    }
  }

  /**
   * The bounds the endpoint honours have to keep being honoured, and the timestamp a combination is reported at
   * is the earliest one INSIDE the range. A block only partly covered by the range carries no such timestamp in
   * its directory entry - {@code minTimestamp} is the block's, not the range's - so it is read.
   */
  @Test
  void honoursTheRangeAndReportsTheEarliestTimestampInsideIt() throws Exception {
    final TimeSeriesEngine engine = engineFor("ts_partial_range");
    try {
      appendAndSeal(engine, "h1", "eu", 1000L, 5);   // 1000..5000
      appendAndSeal(engine, "h2", "us", 10000L, 5);  // 10000..14000

      // Cuts the first block in half and leaves the second one out entirely.
      final Map<String, Long> combinations = foldCombinations(engine, 3000L, 6000L, new AggregationMetrics());

      assertThat(combinations).containsOnlyKeys("h1|eu");
      assertThat(combinations.get("h1|eu"))
          .as("the earliest sample inside the range, not the block's own minTimestamp").isEqualTo(3000L);

      assertThat(combinations).isEqualTo(foldRows(engine, 3000L, 6000L));
    } finally {
      engine.close();
    }
  }

  /**
   * The mutable layer declares nothing, so it is always scanned - and its rows have to fold into the same answer
   * as the sealed ones, including when a series lives in BOTH layers.
   */
  @Test
  void foldsTheMutableBucketTogetherWithTheSealedBlocks() throws Exception {
    final TimeSeriesEngine engine = engineFor("ts_both_layers");
    try {
      appendAndSeal(engine, "h1", "eu", 1000L, 5);
      // Not sealed: stays in the mutable bucket. One row extends a sealed series, one opens a new one.
      append(engine, new long[] { 6000L, 7000L }, new Object[] { "h1", "h3" }, new Object[] { "eu", "ap" },
          new Object[] { 1.0, 2.0 });

      final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE,
          new AggregationMetrics());

      assertThat(combinations).containsOnlyKeys("h1|eu", "h3|ap");
      assertThat(combinations.get("h1|eu"))
          .as("the sealed block is older, so it owns the series' earliest timestamp").isEqualTo(1000L);
      assertThat(combinations.get("h3|ap")).isEqualTo(7000L);

      assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
    } finally {
      engine.close();
    }
  }

  /**
   * A tag column carrying an explicit non-dictionary codec declares its distinct values as TEXT while a scan of
   * it hands back the boxed value, so its declaration cannot stand in for the scan: the block is read instead of
   * being answered with a {@code String} where the caller would have seen an {@code Integer}.
   */
  @Test
  void readsABlockWhoseTagDeclarationIsNotSpelledTheWayAScanSpellsIt() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.INTEGER, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.SIMPLE8B),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ts_numeric_tag", columns, 1);
    database.commit();
    try {
      append(engine, new long[] { 1000L, 2000L }, new Object[] { 7, 7 }, null, new Object[] { 1.0, 2.0 });
      seal(engine);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> visited = new ArrayList<>();
      engine.forEachTagCombination(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 }, metrics, row -> {
        visited.add(row);
        return true;
      });

      assertThat(metrics.getMaterializedRows())
          .as("the declaration is text, the scan gives an Integer: the block has to be read").isEqualTo(2);
      assertThat(visited).allSatisfy(row -> assertThat(row[1]).isEqualTo(7));
    } finally {
      engine.close();
    }
  }

  private TimeSeriesEngine engineFor(final String typeName) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, typeName, twoTagColumns(), 1);
    database.commit();
    return engine;
  }

  /** {@code count} samples one second apart for one (host, region) pair, sealed into their own block. */
  private void appendAndSeal(final TimeSeriesEngine engine, final String host, final String region,
      final long startTs, final int count) throws Exception {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] regions = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = startTs + i * 1000L;
      hosts[i] = host;
      regions[i] = region;
      values[i] = (double) i;
    }
    append(engine, timestamps, hosts, regions, values);
    seal(engine);
  }

  private void append(final TimeSeriesEngine engine, final long[] timestamps, final Object[] hosts,
      final Object[] regions, final Object[] values) throws Exception {
    database.begin();
    if (regions == null)
      engine.appendSamples(timestamps, hosts, values);
    else
      engine.appendSamples(timestamps, hosts, regions, values);
    database.commit();
  }

  private void seal(final TimeSeriesEngine engine) throws Exception {
    database.begin();
    engine.compactAll();
    database.commit();
  }

  /** The answer {@code GetPromQLSeriesHandler} builds, folded off {@code forEachTagCombination}. */
  private Map<String, Long> foldCombinations(final TimeSeriesEngine engine, final long fromTs, final long toTs,
      final AggregationMetrics metrics) throws IOException {
    final Map<String, Long> earliest = new LinkedHashMap<>();
    engine.forEachTagCombination(fromTs, toTs, TAG_PROJECTION, metrics, row -> {
      fold(earliest, row);
      return true;
    });
    return earliest;
  }

  /** The same answer folded off the full scan, which is what it has to equal. */
  private Map<String, Long> foldRows(final TimeSeriesEngine engine, final long fromTs, final long toTs)
      throws IOException {
    final Map<String, Long> earliest = new LinkedHashMap<>();
    engine.forEachRow(fromTs, toTs, TAG_PROJECTION, null, null, row -> {
      fold(earliest, row);
      return true;
    });
    return earliest;
  }

  private static void fold(final Map<String, Long> earliest, final Object[] row) {
    final String key = row[1] + "|" + row[2];
    final long timestamp = (long) row[0];
    earliest.merge(key, timestamp, Math::min);
  }
}
