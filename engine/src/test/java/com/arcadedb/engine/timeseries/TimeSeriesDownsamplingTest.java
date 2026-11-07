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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.AlterTimeSeriesTypeStatement;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.Type;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for TimeSeries downsampling policies.
 */
class TimeSeriesDownsamplingTest extends TestHelper {

  private List<ColumnDefinition> createTestColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @Test
  void ddlAddAndDropPolicy() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorDDL TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");

    // Add downsampling policy
    database.command("sql",
        "ALTER TIMESERIES TYPE SensorDDL ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS");

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("SensorDDL");
    assertThat(type.getDownsamplingTiers()).hasSize(2);
    // Sorted by afterMs ascending
    assertThat(type.getDownsamplingTiers().get(0).afterMs()).isEqualTo(7 * 86400000L);
    assertThat(type.getDownsamplingTiers().get(0).granularityMs()).isEqualTo(3600000L);
    assertThat(type.getDownsamplingTiers().get(1).afterMs()).isEqualTo(30 * 86400000L);
    assertThat(type.getDownsamplingTiers().get(1).granularityMs()).isEqualTo(86400000L);

    // Verify persistence by closing and reopening
    database.close();
    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("SensorDDL");
    assertThat(reopened.getDownsamplingTiers()).hasSize(2);
    assertThat(reopened.getDownsamplingTiers().get(0).afterMs()).isEqualTo(7 * 86400000L);

    // Drop downsampling policy
    database.command("sql", "ALTER TIMESERIES TYPE SensorDDL DROP DOWNSAMPLING POLICY");
    final LocalTimeSeriesType afterDrop = (LocalTimeSeriesType) database.getSchema().getType("SensorDDL");
    assertThat(afterDrop.getDownsamplingTiers()).isEmpty();
  }

  @Test
  void singleTierDownsamplingAccuracy() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_accuracy", columns, 1);

    // Insert 60 samples at 1-second intervals (timestamps 0..59000)
    // All with same sensor, temperature values 1.0, 2.0, ..., 60.0
    final long[] timestamps = new long[60];
    final Object[] sensors = new Object[60];
    final Object[] temps = new Object[60];
    for (int i = 0; i < 60; i++) {
      timestamps[i] = i * 1000L;
      sensors[i] = "sensor_A";
      temps[i] = (double) (i + 1);
    }
    engine.appendSamples(timestamps, sensors, temps);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      // Downsample to 1-minute granularity. Set nowMs such that all data is old enough.
      // afterMs = 1ms means everything older than (nowMs - 1) qualifies
      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, 60000L));
      engine.applyDownsampling(tiers, 60001L);

      // All 60 samples should be aggregated into 1 sample (bucket 0)
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      database.begin();
      final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      assertThat(result).hasSize(1);
      assertThat((long) result.get(0)[0]).isEqualTo(0L); // bucket timestamp
      assertThat((String) result.get(0)[1]).isEqualTo("sensor_A");
      // AVG of 1..60 = 30.5
      assertThat((double) result.get(0)[2]).isCloseTo(30.5, Offset.offset(0.001));
    } finally {
      engine.close();
    }
  }

  @Test
  void multiTierDownsampling() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_multitier", columns, 1);

    // Insert samples spanning multiple time ranges
    // "Old" data: 120 samples at 1-second intervals starting at t=0 (0..119s)
    // "Recent" data: 60 samples at 1-second intervals starting at t=200000 (200s..259s)
    final long[] timestamps = new long[180];
    final Object[] sensors = new Object[180];
    final Object[] temps = new Object[180];
    for (int i = 0; i < 120; i++) {
      timestamps[i] = i * 1000L;
      sensors[i] = "sensor_A";
      temps[i] = 10.0;
    }
    for (int i = 0; i < 60; i++) {
      timestamps[120 + i] = 200000L + i * 1000L;
      sensors[120 + i] = "sensor_A";
      temps[120 + i] = 20.0;
    }
    engine.appendSamples(timestamps, sensors, temps);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      // Tier 1: after 100ms -> 1-minute granularity (affects data older than nowMs-100)
      // Tier 2: after 200ms -> 2-minute granularity (affects data older than nowMs-200)
      final long nowMs = 260000L;
      final List<DownsamplingTier> tiers = List.of(
          new DownsamplingTier(100L, 60000L),
          new DownsamplingTier(200L, 120000L)
      );
      engine.applyDownsampling(tiers, nowMs);

      database.begin();
      final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      // After tier 1 (granularity 60s): old data (0-119s) -> 2 buckets (0, 60000)
      //                                  recent data (200s-259s) -> 1 bucket (200000 rounded = 180000, 240000)
      // After tier 2 (granularity 120s, cutoff 260000-200=260800-200): applies to data older than 260000-200=259800
      // Data at 0 and 60000 qualifies for tier 2 -> downsampled to 120s buckets -> 1 bucket (0)
      // All data values are constant per range, so AVG=10.0 for old, 20.0 for recent
      assertThat(result).isNotEmpty();

      // Verify all old data timestamps are aligned to at least 60s boundaries
      for (final Object[] row : result) {
        final long ts = (long) row[0];
        if (ts < 200000L)
          assertThat(ts % 60000L).isEqualTo(0L);
      }
    } finally {
      engine.close();
    }
  }

  @Test
  void idempotency() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_idempotent", columns, 1);

    // Insert 60 samples at 1-second intervals
    final long[] timestamps = new long[60];
    final Object[] sensors = new Object[60];
    final Object[] temps = new Object[60];
    for (int i = 0; i < 60; i++) {
      timestamps[i] = i * 1000L;
      sensors[i] = "sensor_A";
      temps[i] = (double) (i + 1);
    }
    engine.appendSamples(timestamps, sensors, temps);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, 60000L));

      // First downsampling
      engine.applyDownsampling(tiers, 60001L);

      database.begin();
      final List<Object[]> firstResult = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      final int blockCountAfterFirst = engine.getShard(0).getSealedStore().getBlockCount();

      // Second downsampling (should be a no-op due to density check)
      engine.applyDownsampling(tiers, 60001L);

      database.begin();
      final List<Object[]> secondResult = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(blockCountAfterFirst);
      assertThat(secondResult).hasSize(firstResult.size());
      for (int i = 0; i < firstResult.size(); i++) {
        assertThat((long) secondResult.get(i)[0]).isEqualTo((long) firstResult.get(i)[0]);
        assertThat((double) secondResult.get(i)[2]).isCloseTo((double) firstResult.get(i)[2],
            Offset.offset(0.001));
      }
    } finally {
      engine.close();
    }
  }

  /**
   * Regression for issue #4599: when a prior downsampling collapses every tag-group into a single bucket,
   * the resulting block has maxTs == minTs (zero span) and more than one sample. Its average sample spacing
   * is 0, so the old density check kept re-selecting it for downsampling on every maintenance cycle, wasting
   * CPU/IO by rewriting an identical sealed file forever. After the fix the block carries a downsampling
   * marker and is left untouched on subsequent cycles.
   */
  @Test
  void zeroSpanBlockNotReDownsampledEveryCycle() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_zerospan", columns, 1);

    // Two sensors, three samples each, all inside the same 60s bucket [0,60000).
    final long[] timestamps = { 0, 10000, 20000, 5000, 15000, 25000 };
    final Object[] sensors = { "sensor_A", "sensor_A", "sensor_A", "sensor_B", "sensor_B", "sensor_B" };
    final Object[] temps = { 10.0, 20.0, 30.0, 100.0, 200.0, 300.0 };
    engine.appendSamples(timestamps, sensors, temps);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, 60000L));

      // First downsampling: collapses to one block with two samples, both at bucket ts 0 -> maxTs == minTs.
      engine.applyDownsampling(tiers, 60001L);
      final long rewritesAfterFirst = engine.getShard(0).getSealedStore().getDownsampleRewriteCount();
      assertThat(rewritesAfterFirst).isEqualTo(1L);

      // Sanity: the produced block is genuinely zero-span with more than one sample (the bug trigger).
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);
      assertThat(engine.getShard(0).getSealedStore().getBlockMinTimestamp(0))
          .isEqualTo(engine.getShard(0).getSealedStore().getBlockMaxTimestamp(0));

      // Subsequent cycles must be true no-ops: no further rewrites.
      engine.applyDownsampling(tiers, 60001L);
      engine.applyDownsampling(tiers, 60001L);
      assertThat(engine.getShard(0).getSealedStore().getDownsampleRewriteCount()).isEqualTo(rewritesAfterFirst);

      // Data must remain correct after the repeated cycles.
      database.begin();
      final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      assertThat(result).hasSize(2);
      double avgA = 0, avgB = 0;
      for (final Object[] row : result) {
        assertThat((long) row[0]).isEqualTo(0L);
        if ("sensor_A".equals(row[1]))
          avgA = (double) row[2];
        else if ("sensor_B".equals(row[1]))
          avgB = (double) row[2];
      }
      assertThat(avgA).isCloseTo(20.0, Offset.offset(0.001));
      assertThat(avgB).isCloseTo(200.0, Offset.offset(0.001));
    } finally {
      engine.close();
    }
  }

  /**
   * Regression for issue #4599: applyDownsampling must process tiers oldest-cutoff-first regardless of the
   * order in which they were declared. An unsorted list (coarse-granularity tier declared before a finer one
   * that targets older data) must not cause a block to be downsampled twice in a single pass nor yield a
   * result different from the sorted declaration.
   */
  @Test
  void tierOrderIndependence() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine sortedEngine = new TimeSeriesEngine(db, "ds_tier_sorted", columns, 1);
    final TimeSeriesEngine unsortedEngine = new TimeSeriesEngine(db, "ds_tier_unsorted", columns, 1);

    final long[] timestamps = new long[120];
    final Object[] sensors = new Object[120];
    final Object[] temps = new Object[120];
    for (int i = 0; i < 120; i++) {
      timestamps[i] = i * 1000L;
      sensors[i] = "sensor_A";
      temps[i] = 10.0;
    }
    sortedEngine.appendSamples(timestamps, sensors, temps);
    unsortedEngine.appendSamples(timestamps.clone(), sensors.clone(), temps.clone());
    database.commit();

    try {
      database.begin();
      sortedEngine.compactAll();
      unsortedEngine.compactAll();
      database.commit();

      final long nowMs = 200000L;
      final DownsamplingTier fine = new DownsamplingTier(100L, 60000L);    // newer cutoff, finer granularity
      final DownsamplingTier coarse = new DownsamplingTier(50000L, 120000L); // older cutoff, coarser granularity

      sortedEngine.applyDownsampling(List.of(fine, coarse), nowMs);
      unsortedEngine.applyDownsampling(List.of(coarse, fine), nowMs);

      database.begin();
      final List<Object[]> sorted = sortedEngine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      final List<Object[]> unsorted = unsortedEngine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      assertThat(unsorted).hasSameSizeAs(sorted);
      for (int i = 0; i < sorted.size(); i++) {
        assertThat((long) unsorted.get(i)[0]).isEqualTo((long) sorted.get(i)[0]);
        assertThat((double) unsorted.get(i)[2]).isCloseTo((double) sorted.get(i)[2], Offset.offset(0.001));
      }
    } finally {
      sortedEngine.close();
      unsortedEngine.close();
    }
  }

  @Test
  void multiTagGrouping() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_multitag", columns, 1);

    // Insert samples from two sensors in the same time bucket (0-59s)
    final long[] timestamps = new long[6];
    final Object[] sensors = new Object[6];
    final Object[] temps = new Object[6];

    // sensor_A: temps 10, 20, 30 -> avg 20
    timestamps[0] = 0;     sensors[0] = "sensor_A"; temps[0] = 10.0;
    timestamps[1] = 10000; sensors[1] = "sensor_A"; temps[1] = 20.0;
    timestamps[2] = 20000; sensors[2] = "sensor_A"; temps[2] = 30.0;
    // sensor_B: temps 100, 200, 300 -> avg 200
    timestamps[3] = 5000;  sensors[3] = "sensor_B"; temps[3] = 100.0;
    timestamps[4] = 15000; sensors[4] = "sensor_B"; temps[4] = 200.0;
    timestamps[5] = 25000; sensors[5] = "sensor_B"; temps[5] = 300.0;

    engine.appendSamples(timestamps, sensors, temps);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, 60000L));
      engine.applyDownsampling(tiers, 60001L);

      database.begin();
      final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      // Should produce 2 samples: one per sensor, both at bucket timestamp 0
      assertThat(result).hasSize(2);

      // Both at timestamp 0
      assertThat((long) result.get(0)[0]).isEqualTo(0L);
      assertThat((long) result.get(1)[0]).isEqualTo(0L);

      // Find sensor_A and sensor_B results
      double avgA = 0, avgB = 0;
      for (final Object[] row : result) {
        if ("sensor_A".equals(row[1]))
          avgA = (double) row[2];
        else if ("sensor_B".equals(row[1]))
          avgB = (double) row[2];
      }
      assertThat(avgA).isCloseTo(20.0, Offset.offset(0.001));
      assertThat(avgB).isCloseTo(200.0, Offset.offset(0.001));
    } finally {
      engine.close();
    }
  }

  @Test
  void interactionWithRetention() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_retention", columns, 1);

    // Insert old data and recent data
    engine.appendSamples(
        new long[] { 1000, 2000, 3000 },
        new Object[] { "sensor_A", "sensor_A", "sensor_A" },
        new Object[] { 10.0, 20.0, 30.0 }
    );
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      database.begin();
      engine.appendSamples(
          new long[] { 100000, 200000, 300000 },
          new Object[] { "sensor_A", "sensor_A", "sensor_A" },
          new Object[] { 100.0, 200.0, 300.0 }
      );
      database.commit();

      database.begin();
      engine.compactAll();
      database.commit();

      // Apply retention first: remove blocks with maxTs < 50000
      engine.applyRetention(50000L);
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      // Apply downsampling on remaining data
      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, 200000L));
      engine.applyDownsampling(tiers, 400000L);

      database.begin();
      final List<Object[]> result = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();

      // Remaining data should be downsampled without errors
      assertThat(result).isNotEmpty();
    } finally {
      engine.close();
    }
  }

  @Test
  void noOpOnEmptyEngine() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_empty", columns, 1);
    database.commit();

    try {
      // Should not throw with empty data
      engine.applyDownsampling(List.of(new DownsamplingTier(1L, 60000L)), 100000L);
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(0);

      // Should not throw with null/empty tier list
      engine.applyDownsampling(null, 100000L);
      engine.applyDownsampling(List.of(), 100000L);
    } finally {
      engine.close();
    }
  }

  // ---- #7711 ----
  // Issue #7711: {@code TimeSeriesSealedStore.downsampleBlocks} decompressed the tag columns of the blocks it was
  // about to rewrite through a codec switch whose only case was {@code DICTIONARY}. Its {@code default} arm did not
  // decode the column, it DISCARDED it - yielding an array of nulls, which the grouping key a few lines down turned
  // into the empty string.
  // <p>
  // The rows were then gone rather than mislabelled: every distinct value of that tag merged into one group, the
  // rewritten blocks carried {@code ""} where the tag value had been, and the recomputed distinct-value declaration
  // was built from those same rewritten rows - so it agreed with the damage and nothing downstream could see it.
  // <p>
  // A TAG column defaults to {@code DICTIONARY} ({@code ColumnDefinition.defaultCodecFor}), so reaching this needs
  // a column given an EXPLICIT codec, which {@code TimeSeriesTypeBuilder.withColumn(ColumnDefinition)} allows since
  // #7399. That makes it latent, but it is latent DATA LOSS on a background maintenance path.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  /** One minute of one-second samples, downsampled into a single one-minute bucket per tag value. */
  private static final int  SAMPLE_COUNT   = 60;

  private static final long GRANULARITY_MS = 60_000L;

  // #7711
  /**
   * An INTEGER tag stored with {@code SIMPLE8B}: the shape the issue names. Two zones interleaved sample by
   * sample must come out of downsampling as two series, not as one series tagged with the empty string.
   */
  @Test
  void keepsTheSeriesOfASimple8bTagColumnSeparate() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.INTEGER, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.SIMPLE8B),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final TimeSeriesEngine engine = sealOneBlock("ds_simple8b_tag", columns, i -> (i % 2) + 1);
    try {
      final List<Object[]> downsampled = downsample(engine);

      // Two zones in, two zones out. Before the fix this was ONE row, tagged "".
      assertThat(downsampled).hasSize(2);
      assertThat(distinctTagValues(downsampled)).containsExactlyInAnyOrder(1, 2);

      for (final Object[] row : downsampled) {
        assertThat((long) row[0]).as("both series fall in the same one-minute bucket").isZero();
        // zone 1 carries the even-indexed samples (1, 3, ... 59), zone 2 the odd-indexed ones (2, 4, ... 60).
        final double expected = (int) row[1] == 1 ? 30.0 : 31.0;
        assertThat((double) row[2]).isCloseTo(expected, Offset.offset(0.001));
      }

      assertThat(engine.getShard(0).getSealedStore().checkIntegrity())
          .as("the rewritten blocks must declare the tag values they actually hold").isEmpty();
    } finally {
      engine.close();
    }
  }

  // #7711
  /**
   * The same defect through the other numeric codec a tag column can be given, so the fix is the codec table
   * rather than one extra case: {@code GORILLA_XOR} on a DOUBLE tag.
   */
  @Test
  void keepsTheSeriesOfAGorillaTagColumnSeparate() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.DOUBLE, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.GORILLA_XOR),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final TimeSeriesEngine engine = sealOneBlock("ds_gorilla_tag", columns, i -> (double) ((i % 2) + 1));
    try {
      final List<Object[]> downsampled = downsample(engine);

      assertThat(downsampled).hasSize(2);
      assertThat(distinctTagValues(downsampled)).containsExactlyInAnyOrder(1.0, 2.0);
    } finally {
      engine.close();
    }
  }

  // #7711
  /**
   * The control: a tag column on its DEFAULT codec is what every existing downsampling test uses, and it must
   * keep answering exactly as it did. This is the case the old switch handled, so a regression here would mean
   * the shared decoder boxes a dictionary value differently from the copy it replaced.
   */
  @Test
  void leavesADictionaryTagColumnAnswering() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("zone", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    final TimeSeriesEngine engine = sealOneBlock("ds_dictionary_tag", columns, i -> "zone_" + (i % 2));
    try {
      final List<Object[]> downsampled = downsample(engine);

      assertThat(downsampled).hasSize(2);
      assertThat(distinctTagValues(downsampled)).containsExactlyInAnyOrder("zone_0", "zone_1");
    } finally {
      engine.close();
    }
  }

  // #7711
  /**
   * Why {@code decodeColumn}'s new {@code default} arm is unreachable rather than merely believed to be
   * (code review on PR #7730). It throws for a codec it cannot decode, which would turn a previously-silent
   * corruption into a hard failure on a background maintenance path - so it matters that no sealed block can
   * carry such a column in the first place.
   * <p>
   * It cannot, because the WRITE side refuses first and always has: {@code compressColumn} throws for the same
   * two codecs, so a type declaring one never gets as far as producing a block for the reader to choke on. The
   * type itself is created without complaint - {@code TimeSeriesTypeBuilder} does not restrict codec by role,
   * which {@code Issue7399TimeSeriesTypeBuilderSQLTest} pins - and the refusal lands at seal time.
   */
  @Test
  void aColumnWithAnUndecodableCodecIsRefusedAtSealTimeSoNoBlockCanCarryOne() throws Exception {
    for (final TimeSeriesCodec codec : new TimeSeriesCodec[] { TimeSeriesCodec.NONE,
        TimeSeriesCodec.DELTA_OF_DELTA }) {
      final List<ColumnDefinition> columns = List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("zone", Type.STRING, ColumnDefinition.ColumnRole.TAG, codec),
          new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

      final DatabaseInternal db = (DatabaseInternal) database;
      database.begin();
      final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_undecodable_" + codec, columns, 1);
      database.commit();
      try {
        database.begin();
        engine.appendSamples(new long[] { 1000L, 2000L }, new Object[] { "a", "b" }, new Object[] { 1.0, 2.0 });
        database.commit();

        assertThatThrownBy(() -> {
          database.begin();
          try {
            engine.compactAll();
          } finally {
            database.commit();
          }
        }).as("%s reaches compressColumn before any reader could meet it", codec)
            .hasRootCauseInstanceOf(IllegalStateException.class)
            .rootCause().hasMessageContaining(codec.name());
      } finally {
        engine.close();
      }
    }
  }

  /** The tag value of sample {@code i}. */
  private interface TagValues {
    Object at(int i);
  }

  /**
   * Appends {@link #SAMPLE_COUNT} samples one second apart and compacts them into a single sealed block, which is
   * what downsampling reads. {@code temperature} is {@code i + 1}, so each tag's average is known in advance.
   */
  private TimeSeriesEngine sealOneBlock(final String typeName, final List<ColumnDefinition> columns,
      final TagValues tags) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, typeName, columns, 1);

    final long[] timestamps = new long[SAMPLE_COUNT];
    final Object[] zones = new Object[SAMPLE_COUNT];
    final Object[] temperatures = new Object[SAMPLE_COUNT];
    for (int i = 0; i < SAMPLE_COUNT; i++) {
      timestamps[i] = i * 1000L;
      zones[i] = tags.at(i);
      temperatures[i] = (double) (i + 1);
    }
    engine.appendSamples(timestamps, zones, temperatures);
    database.commit();

    database.begin();
    engine.compactAll();
    database.commit();

    assertThat(engine.getShard(0).getSealedStore().getBlockCount())
        .as("the samples have to be SEALED for downsampling to see them").isEqualTo(1);
    return engine;
  }

  /** Downsamples everything to one bucket per minute and reads the result back. */
  private List<Object[]> downsample(final TimeSeriesEngine engine) throws Exception {
    engine.applyDownsampling(List.of(new DownsamplingTier(1L, GRANULARITY_MS)), SAMPLE_COUNT * 1000L + 1L);

    database.begin();
    try {
      return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    } finally {
      database.commit();
    }
  }

  /** The tag values the rows carry, as the read path hands them back. */
  private static Set<Object> distinctTagValues(final List<Object[]> rows) {
    final Set<Object> values = new LinkedHashSet<>();
    for (final Object[] row : rows)
      values.add(row[1]);
    return values;
  }

  // ---- #9800 ----
  // Issue #9800: a TIMESERIES type's declared precision was accepted and then ignored by every time computation, all of
  // which count in milliseconds - for {@code PRECISION SECOND} the first retention pass deleted every sample. Only
  // MILLISECOND can be declared now; a type declared otherwise before the fix keeps loading, with retention and
  // downsampling suspended until {@code ALTER TIMESERIES TYPE ... PRECISION MILLISECOND} confirms the unit.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long DAY_MS = 86_400_000L;

  // #9800
  @Test
  void createRefusesEveryPrecisionButMillisecond() {
    for (final String unit : new String[] { "SECOND", "MICROSECOND", "NANOSECOND" })
      assertThatThrownBy(() -> database.command("sql",
          "CREATE TIMESERIES TYPE p_" + unit + " TIMESTAMP ts PRECISION " + unit + " FIELDS (v DOUBLE)")).as(unit)
          .isInstanceOf(SchemaException.class).hasMessageContaining(unit).hasMessageContaining("MILLISECOND");

    database.command("sql", "CREATE TIMESERIES TYPE p_ms TIMESTAMP ts PRECISION MILLISECOND FIELDS (v DOUBLE)");
    final TimeSeriesType ms = (TimeSeriesType) database.getSchema().getType("p_ms");
    assertThat(ms.getPrecision()).isEqualTo("MILLISECOND");
    assertThat(ms.isPrecisionHonoured()).isTrue();

    database.command("sql", "CREATE TIMESERIES TYPE p_default TIMESTAMP ts FIELDS (v DOUBLE)");
    assertThat(((TimeSeriesType) database.getSchema().getType("p_default")).isPrecisionHonoured()).isTrue();
  }

  // #9800
  @Test
  void theBuilderRefusesItEmbeddedAndAsSQL() {
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("b").withTimestamp("ts")
        .withPrecision("second").withField("v", Type.DOUBLE).create()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("SECOND");
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("b").withTimestamp("ts")
        .withPrecision("NANOSECOND").withField("v", Type.DOUBLE).toSQL()).isInstanceOf(SchemaException.class);

    // A restore keeps a legacy declaration embedded, and cannot render it as DDL
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("b").withTimestamp("ts")
        .withLegacyPrecision("NANOSECOND").withField("v", Type.DOUBLE).toSQL()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("embedded");
    assertThat(database.getSchema().existsType("b")).isFalse();
  }

  // #9800
  @Test
  void aLegacySecondTypeIsNotWipedByTheRetentionPass() {
    final LocalTimeSeriesType type = legacyType("sec_metric", "SECOND");
    final long nowSec = System.currentTimeMillis() / 1000;
    for (int i = 0; i < 5; i++)
      insert("sec_metric", nowSec - i);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "sec_metric");

    // Before the fix, the millisecond cutoff (~1.8e12) was larger than every SECOND timestamp (~1.8e9): 0 rows
    assertThat(count("sec_metric")).isEqualTo(5L);
  }

  // #9800
  @Test
  void alterPrecisionMillisecondResumesTheSuspendedRetention() throws Exception {
    // Declared NANOSECOND, but fed milliseconds - what every ingestion API (line protocol, Prometheus, gRPC) wrote
    final LocalTimeSeriesType type = legacyType("ns_metric", "NANOSECOND");
    final long nowMs = System.currentTimeMillis();
    insert("ns_metric", nowMs - 3 * DAY_MS);
    // Retention drops whole sealed blocks, so the old sample is sealed in a block of its own
    type.getEngine().compactAll();
    for (int i = 0; i < 4; i++)
      insert("ns_metric", nowMs - i * 1000L);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "ns_metric");
    assertThat(count("ns_metric")).as("retention suspended").isEqualTo(5L);
    assertThat(maintenanceSuspended("ns_metric")).isTrue();

    try (final ResultSet rs = database.command("sql", "ALTER TIMESERIES TYPE ns_metric PRECISION MILLISECOND")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("previousPrecision")).isEqualTo("NANOSECOND");
      assertThat(r.<String>getProperty("precision")).isEqualTo("MILLISECOND");
    }
    assertThat(type.isPrecisionHonoured()).isTrue();
    assertThat(maintenanceSuspended("ns_metric")).isFalse();

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "ns_metric");
    assertThat(count("ns_metric")).as("the 3-day-old sample is past the 1-day retention").isEqualTo(4L);

    // Persisted with the schema
    reopenDatabase();
    assertThat(((TimeSeriesType) database.getSchema().getType("ns_metric")).getPrecision()).isEqualTo("MILLISECOND");
  }

  // #9800
  @Test
  void alterPrecisionAcceptsOnlyMillisecond() {
    database.command("sql", "CREATE TIMESERIES TYPE alt TIMESTAMP ts FIELDS (v DOUBLE)");
    assertThatThrownBy(() -> database.command("sql", "ALTER TIMESERIES TYPE alt PRECISION SECOND"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("MILLISECOND");

    // Idempotent on a type already in milliseconds
    try (final ResultSet rs = database.command("sql", "ALTER TIMESERIES TYPE alt PRECISION MILLISECOND")) {
      assertThat(rs.next().<String>getProperty("previousPrecision")).isNull();
    }
    assertThat(((TimeSeriesType) database.getSchema().getType("alt")).getPrecision()).isEqualTo("MILLISECOND");
  }

  // #9800
  @Test
  void alterPrecisionRendersAndReparses() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final AlterTimeSeriesTypeStatement stmt = (AlterTimeSeriesTypeStatement) parser.parse(
        "ALTER TIMESERIES TYPE alt PRECISION millisecond");
    assertThat(stmt.precision).isEqualTo("MILLISECOND");
    assertThat(stmt.toString()).isEqualTo("ALTER TIMESERIES TYPE alt PRECISION MILLISECOND");
    assertThat(parser.parse(stmt.toString())).isEqualTo(stmt);
    assertThat(stmt.copy()).isEqualTo(stmt);
  }

  private LocalTimeSeriesType legacyType(final String name, final String precision) {
    // The way a type declared before #9800 reaches this build: a schema that already carries the precision
    return (LocalTimeSeriesType) database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts")
        .withLegacyPrecision(precision).withTag("host", Type.STRING).withField("v", Type.DOUBLE).withShards(1)
        .withRetention(DAY_MS).create();
  }

  private void insert(final String type, final long ts) {
    database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = 'h1', v = 1.0", ts));
  }

  private boolean maintenanceSuspended(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT maintenanceSuspended FROM schema:types WHERE name = ?", type)) {
      return Boolean.TRUE.equals(rs.next().getProperty("maintenanceSuspended"));
    }
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
