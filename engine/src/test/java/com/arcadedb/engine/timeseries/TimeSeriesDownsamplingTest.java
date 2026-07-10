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
import com.arcadedb.schema.Type;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
    assertThat(type.getDownsamplingTiers().getFirst().afterMs()).isEqualTo(7 * 86400000L);
    assertThat(type.getDownsamplingTiers().getFirst().granularityMs()).isEqualTo(3600000L);
    assertThat(type.getDownsamplingTiers().get(1).afterMs()).isEqualTo(30 * 86400000L);
    assertThat(type.getDownsamplingTiers().get(1).granularityMs()).isEqualTo(86400000L);

    // Verify persistence by closing and reopening
    database.close();
    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType("SensorDDL");
    assertThat(reopened.getDownsamplingTiers()).hasSize(2);
    assertThat(reopened.getDownsamplingTiers().getFirst().afterMs()).isEqualTo(7 * 86400000L);

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

    try (engine) {
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
      assertThat((long) result.getFirst()[0]).isEqualTo(0L); // bucket timestamp
      assertThat((String) result.getFirst()[1]).isEqualTo("sensor_A");
      // AVG of 1..60 = 30.5
      assertThat((double) result.getFirst()[2]).isCloseTo(30.5, Offset.offset(0.001));
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

    try (engine) {
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

    try (engine) {
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

    try (engine) {
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

    try (sortedEngine) {
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

    try (engine) {
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
      assertThat((long) result.getFirst()[0]).isEqualTo(0L);
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

    try (engine) {
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
    }
  }

  @Test
  void noOpOnEmptyEngine() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ds_empty", columns, 1);
    database.commit();

    try (engine) {
      // Should not throw with empty data
      engine.applyDownsampling(List.of(new DownsamplingTier(1L, 60000L)), 100000L);
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(0);

      // Should not throw with null/empty tier list
      engine.applyDownsampling(null, 100000L);
      engine.applyDownsampling(List.of(), 100000L);
    }
  }
}
