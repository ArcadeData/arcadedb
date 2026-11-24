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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression test for issue #11984: a downsampling tier skipped every raw sealed block whose AVERAGE sample spacing was at least the
 * granularity, reading it as "already at the target resolution". Nine samples of one series inside one hour plus a tenth nine hours
 * later average one hour of spacing, so the nine stayed raw forever under a one-hour tier, while the same nine alone were downsampled.
 * <p>
 * A block is downsampled when that reduces it, that is when two of its samples share a (tag values, bucket); the tests below pin that
 * from both sides, and what it means for the replication: every rewrite leaves fewer samples, so the shipping decision of the engine,
 * which reads the counts, sees every one of them.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11984DownsampleClusteredBlockTest extends TestHelper {

  private static final long H = 3_600_000L;

  @Test
  void aBlockWhoseSamplesShareABucketIsDownsampledWhateverItsAverageSpacing() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984", columns(), 1);
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
   * A block with one sample per (tag values, bucket) has nothing to fold: it stays as it is, with its raw timestamps, and nothing is
   * handed to the replication. Before, the pass moved every timestamp to its bucket start and left as many blocks and samples as it
   * found, so the followers, which never downsample by themselves and are told of a rewrite by its counts, kept answering the raw
   * timestamps the leader no longer held.
   */
  @Test
  void aBlockWithOneSamplePerBucketIsLeftAsItIsAndNothingIsShipped() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984_sparse", columns(), 1);
    engine.appendSamples(new long[] { 30 * 60_000L, H + 10 * 60_000L, 2 * H + 20 * 60_000L }, new Object[] { "a", "a", "a" },
        new Object[] { 1.0, 2.0, 3.0 });
    database.commit();

    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal wrapped = local.getWrappedDatabaseInstance();
    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final AtomicInteger shipped = countShippedSealedFiles(local, wrapped);
      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, H));
      engine.applyDownsampling(tiers, 10 * H);
      engine.applyDownsampling(tiers, 10 * H);

      assertThat(engine.getShard(0).getSealedStore().getDownsampleRewriteCount()).isZero();
      assertThat(shipped.get()).as("sealed files handed to the replication").isZero();
      assertThat(rows(engine)).containsExactly(row(30 * 60_000L, 1.0), row(H + 10 * 60_000L, 2.0), row(2 * H + 20 * 60_000L, 3.0));
    } finally {
      local.setWrappedDatabaseInstance(wrapped);
      engine.close();
    }
  }

  /** A rewrite that reduced the block is handed to the replication, once: the next pass finds nothing to fold and ships nothing. */
  @Test
  void aDownsampleThatReducedABlockIsHandedToTheReplicationOnce() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984_ship", columns(), 1);
    engine.appendSamples(new long[] { 10 * 60_000L, 20 * 60_000L, 30 * 60_000L, 9 * H }, new Object[] { "a", "a", "a", "a" },
        new Object[] { 1.0, 2.0, 3.0, 100.0 });
    database.commit();

    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal wrapped = local.getWrappedDatabaseInstance();
    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final AtomicInteger shipped = countShippedSealedFiles(local, wrapped);
      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, H));
      engine.applyDownsampling(tiers, 10 * H);

      assertThat(rows(engine)).containsExactly(row(0L, 2.0), row(9 * H, 100.0));
      assertThat(shipped.get()).as("sealed files handed to the replication by a downsample that reduced a block").isEqualTo(1);

      engine.applyDownsampling(tiers, 10 * H);
      assertThat(shipped.get()).isEqualTo(1);
    } finally {
      local.setWrappedDatabaseInstance(wrapped);
      engine.close();
    }
  }

  /**
   * What a restart costs the first pass of a tier is a read of the old history to prove its blocks irreducible, and that read must not
   * run inside the rewrite window of the shard ({@link TimeSeriesShard#runSealedRewrite}): that window holds the compaction WRITE lock,
   * so no append and no query of the shard goes by for as long as the history takes to read. A query holds the compaction read lock
   * here, which parks the rewrite of the pass at the write lock, and by the time it is parked the blocks have to be proved already.
   */
  @Test
  @Timeout(120)
  void theProofThatABlockCannotBeReducedIsNotReadInsideTheRewriteWindowOfTheShard() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ds_11984_window", columns(), 1);
    engine.appendSamples(new long[] { 30 * 60_000L, H + 10 * 60_000L, 2 * H + 20 * 60_000L }, new Object[] { "a", "a", "a" },
        new Object[] { 1.0, 2.0, 3.0 });
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      final TimeSeriesShard shard = engine.getShard(0);
      final TimeSeriesSealedStore.DownsampleGrid grid = new TimeSeriesSealedStore.DownsampleGrid(H, 0L);
      assertThat(shard.getSealedStore().snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks()).as("a block nobody has read")
          .extracting(block -> block.isIrreducibleOn(grid)).containsExactly(false);

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(1L, H));
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread pass = new Thread(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        try {
          engine.applyDownsampling(tiers, 10 * H);
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        }
      }, "downsampling-pass-11984");

      final ReentrantReadWriteLock compactionLock = (ReentrantReadWriteLock) shard.getCompactionLock();
      compactionLock.readLock().lock();
      try {
        pass.start();
        // the pass is parked at the write lock of the shard: the one point of its rewrite window that a reader holds back
        await().atMost(Duration.ofSeconds(60)).until(compactionLock::hasQueuedThreads);

        assertThat(shard.getSealedStore().snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
            .as("the blocks were proved before the pass asked for the write lock of the shard")
            .extracting(block -> block.isIrreducibleOn(grid)).containsExactly(true);
      } finally {
        compactionLock.readLock().unlock();
      }
      pass.join();
      assertThat(failure.get()).isNull();
      assertThat(shard.getSealedStore().getDownsampleRewriteCount()).isZero();
    } finally {
      engine.close();
    }
  }

  /**
   * Counts the sealed files the engine hands to the replication; the database says it is replicated, since only a replicated one
   * is handed any, and answers everything else as it did.
   */
  private static AtomicInteger countShippedSealedFiles(final LocalDatabase local, final DatabaseInternal wrapped) {
    final AtomicInteger shipped = new AtomicInteger();
    local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("recordTimeSeriesSealedChange".equals(method.getName())) {
            shipped.incrementAndGet();
            return null;
          }
          if ("isReplicated".equals(method.getName()))
            return true;
          try {
            return method.invoke(wrapped, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        }));
    return shipped;
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
