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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11984: a downsampling tier skipped every raw sealed block whose AVERAGE sample spacing was at least the
 * granularity, reading it as "already at the target resolution". Nine samples of one series inside one hour plus a tenth nine hours
 * later average one hour of spacing, so the nine stayed raw forever under a one-hour tier, while the same nine alone were downsampled.
 */
class Issue11984DownsampleClusteredBlockTest extends TestHelper {

  private static final long H = 3_600_000L;

  @Test
  void aBlockWhoseSamplesShareABucketIsDownsampledWhateverItsAverageSpacing() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984", columns, 1);
    final long[] timestamps = new long[10];
    final Object[] hosts = new Object[10];
    final Object[] values = new Object[10];
    for (int i = 0; i < 9; i++) {
      timestamps[i] = i * 60_000L;
      hosts[i] = "a";
      values[i] = (double) i;
    }
    timestamps[9] = 9 * H;
    hosts[9] = "a";
    values[9] = 100.0;
    engine.appendSamples(timestamps, hosts, values);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, H));
      engine.applyDownsampling(tiers, 10 * H);

      database.begin();
      final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      database.commit();
      assertThat(rows).hasSize(2);
      assertThat((long) rows.get(0)[0]).isEqualTo(0L);
      assertThat((double) rows.get(0)[2]).isEqualTo(4.0); // mean of 0..8
      assertThat((long) rows.get(1)[0]).isEqualTo(9 * H);
      assertThat((double) rows.get(1)[2]).isEqualTo(100.0);

      // the rewritten block carries the marker, so the next pass leaves it alone
      final long rewrites = engine.getShard(0).getSealedStore().getDownsampleRewriteCount();
      engine.applyDownsampling(tiers, 10 * H);
      assertThat(engine.getShard(0).getSealedStore().getDownsampleRewriteCount()).isEqualTo(rewrites);
    } finally {
      engine.close();
    }
  }

  /**
   * The marker is the only thing that tells a pass a block is done, so it has to tell a coarser tier it is not: a block downsampled to
   * one hour is downsampled again by a four-hour tier, and the one-hour tier then leaves the four-hour block alone.
   */
  @Test
  void aBlockDownsampledByAFinerTierIsDownsampledAgainByACoarserOne() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984_tiers", columns(), 1);
    engine.appendSamples(new long[] { 10 * 60_000L, 20 * 60_000L, H + 5 * 60_000L, 5 * H, 9 * H },
        new Object[] { "a", "a", "a", "a", "a" }, new Object[] { 1.0, 3.0, 10.0, 7.0, 100.0 });
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final List<DownsamplingTier> hourly = List.of(new DownsamplingTier(1L, H));
      engine.applyDownsampling(hourly, 10 * H);
      assertThat(rows(engine)).containsExactly(row(0L, 2.0), row(H, 10.0), row(5 * H, 7.0), row(9 * H, 100.0));

      engine.applyDownsampling(List.of(new DownsamplingTier(1L, 4 * H)), 13 * H);
      assertThat(rows(engine)).containsExactly(row(0L, 6.0), row(4 * H, 7.0), row(8 * H, 100.0));

      // a block at four hours is coarser than the one-hour tier asks for: left alone
      final long rewrites = engine.getShard(0).getSealedStore().getDownsampleRewriteCount();
      engine.applyDownsampling(hourly, 13 * H);
      assertThat(engine.getShard(0).getSealedStore().getDownsampleRewriteCount()).isEqualTo(rewrites);
      assertThat(rows(engine)).containsExactly(row(0L, 6.0), row(4 * H, 7.0), row(8 * H, 100.0));
    } finally {
      engine.close();
    }
  }

  /**
   * A block with one sample per bucket is rewritten with as many blocks and samples as it had: only the timestamps move, to the
   * start of their bucket. The rewritten file is handed to the replication all the same, or the followers, which never downsample
   * by themselves, keep answering the raw timestamps the leader no longer holds.
   */
  @Test
  void aDownsampleThatKeepsEverySampleIsStillHandedToTheReplication() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984_ship", columns(), 1);
    engine.appendSamples(new long[] { 30 * 60_000L, H + 10 * 60_000L, 2 * H + 20 * 60_000L }, new Object[] { "a", "a", "a" },
        new Object[] { 1.0, 2.0, 3.0 });
    database.commit();

    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal wrapped = local.getWrappedDatabaseInstance();
    final AtomicInteger shipped = new AtomicInteger();
    try {
      database.begin();
      engine.compactAll();
      database.commit();
      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final int blocks = sealed.getBlockCount();
      final long samples = sealed.getTotalSampleCount();

      local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("recordTimeSeriesSealedChange".equals(method.getName())) {
              shipped.incrementAndGet();
              return null;
            }
            try {
              return method.invoke(wrapped, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          }));

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, H));
      engine.applyDownsampling(tiers, 10 * H);

      // the control: the rewrite kept the counts, which is what the shipping decision used to read
      assertThat(sealed.getBlockCount()).isEqualTo(blocks);
      assertThat(sealed.getTotalSampleCount()).isEqualTo(samples);
      assertThat(rows(engine)).containsExactly(row(0L, 1.0), row(H, 2.0), row(2 * H, 3.0));
      assertThat(shipped.get()).as("sealed files handed to the replication by a downsample that kept every sample").isEqualTo(1);

      // nothing left to rewrite: nothing shipped
      engine.applyDownsampling(tiers, 10 * H);
      assertThat(shipped.get()).isEqualTo(1);
    } finally {
      local.setWrappedDatabaseInstance(wrapped);
      engine.close();
    }
  }

  private static List<ColumnDefinition> columns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  private List<Object[]> rows(final TimeSeriesEngine engine) throws Exception {
    database.begin();
    try {
      return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    } finally {
      database.commit();
    }
  }

  private static Object[] row(final long timestamp, final double value) {
    return new Object[] { timestamp, "a", value };
  }
}
