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
 * A compaction reads the mutable pages of a shard, sorts them and compresses them into sealed blocks. It does so one window of
 * pages at a time ({@link TimeSeriesShard#COMPACTION_WINDOW_SAMPLES} samples), appending the blocks of each window to the temp
 * file before it reads the next, so what it holds in heap does not grow with the number of samples waiting in the mutable
 * bucket. These tests pin that bound, read from a counter of the bucket and never from a clock or the heap, and that the
 * result is the one a single pass over all the samples gives: every sample, in order, cut into full blocks.
 * <p>
 * They also pin that the rewritten sealed file is read whole for the replication only on a replicated database.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TimeSeriesCompactionWindowTest extends TestHelper {

  private static final int WINDOW = TimeSeriesShard.COMPACTION_WINDOW_SAMPLES;
  private static final int BATCH  = 20_000;

  private static List<ColumnDefinition> columns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  /** Samples well past three windows, in order: the compaction never decodes more than one window of them at once. */
  @Test
  void aCompactionHoldsOneWindowOfMutableSamplesAtATime() throws Exception {
    final int total = 3 * WINDOW + 12_345;
    final TimeSeriesShard shard = new TimeSeriesShard((DatabaseInternal) database, "compaction_window", 0, columns());
    try {
      append(shard, 0, total, false);
      assertThat(mutablePages(shard)).as("the samples fill many pages").isGreaterThan(10);

      shard.compact();

      assertThat(shard.getMutableBucket().getLargestCompactionRead())
          .as("samples decoded at once: one window, not the %d waiting in the mutable bucket", total)
          .isPositive()
          .isLessThanOrEqualTo(WINDOW);
      assertThat(mutableSamples(shard)).isZero();

      final TimeSeriesSealedStore sealed = shard.getSealedStore();
      assertThat(sealed.getTotalSampleCount()).isEqualTo(total);
      // the window boundaries leave no short block behind: every block but the last is a full one, as in one pass
      final List<TimeSeriesSealedStore.BlockEntry> blocks = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks();
      assertThat(blocks).hasSize((total + TimeSeriesShard.SEALED_BLOCK_SIZE - 1) / TimeSeriesShard.SEALED_BLOCK_SIZE);
      for (int i = 0; i < blocks.size() - 1; i++)
        assertThat(blocks.get(i).sampleCount).isEqualTo(TimeSeriesShard.SEALED_BLOCK_SIZE);

      assertAllSamplesInOrder(shard, total);
    } finally {
      shard.close();
    }
  }

  /**
   * Samples that arrive out of order across windows, on a bucket-aligned shard: none is lost or duplicated, none is left in the
   * mutable bucket, and no block spans two buckets.
   */
  @Test
  void outOfOrderSamplesAcrossWindowsAreAllSealed() throws Exception {
    final long bucketMs = 50_000L;
    final int total = 2 * WINDOW + 777;
    final TimeSeriesShard shard = new TimeSeriesShard((DatabaseInternal) database, "compaction_window_ooo", 0, columns(), bucketMs);
    try {
      append(shard, 0, total, true);

      shard.compact();

      assertThat(shard.getMutableBucket().getLargestCompactionRead()).isLessThanOrEqualTo(WINDOW);
      assertThat(mutableSamples(shard)).isZero();
      final TimeSeriesSealedStore sealed = shard.getSealedStore();
      assertThat(sealed.getTotalSampleCount()).isEqualTo(total);
      for (final TimeSeriesSealedStore.BlockEntry block : sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks())
        assertThat(Math.floorDiv(block.maxTimestamp, bucketMs)).as("a block stays inside one bucket")
            .isEqualTo(Math.floorDiv(block.minTimestamp, bucketMs));

      assertAllSamplesInOrder(shard, total);
    } finally {
      shard.close();
    }
  }

  /** A second compaction keeps the blocks of the first and adds the new ones. */
  @Test
  void aSecondCompactionKeepsTheBlocksOfTheFirst() throws Exception {
    final int first = WINDOW + 1_000;
    final int second = WINDOW + 2_000;
    final TimeSeriesShard shard = new TimeSeriesShard((DatabaseInternal) database, "compaction_window_twice", 0, columns());
    try {
      append(shard, 0, first, false);
      shard.compact();
      append(shard, first, second, false);
      shard.compact();

      assertThat(shard.getSealedStore().getTotalSampleCount()).isEqualTo(first + second);
      assertThat(shard.getSealedStore().checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      assertAllSamplesInOrder(shard, first + second);
    } finally {
      shard.close();
    }
  }

  /** A standalone database ships nothing, so the compaction does not hand the rewritten sealed file over at all. */
  @Test
  void aStandaloneCompactionHandsNoSealedFileToTheReplication() throws Exception {
    assertThat(sealedFilesHandedOver(false)).isZero();
  }

  /** A replicated database still receives the rewritten sealed file of the compaction. */
  @Test
  void aReplicatedCompactionHandsTheSealedFileToTheReplication() throws Exception {
    assertThat(sealedFilesHandedOver(true)).isEqualTo(1);
  }

  // ---- Helpers ----

  /** Compacts one shard behind a database that counts the sealed files handed to it and says it is replicated or not. */
  private int sealedFilesHandedOver(final boolean replicated) throws Exception {
    final TimeSeriesShard shard = new TimeSeriesShard((DatabaseInternal) database, replicated ? "handover_ha" : "handover_local", 0,
        columns());
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal wrapped = local.getWrappedDatabaseInstance();
    final AtomicInteger handedOver = new AtomicInteger();
    try {
      append(shard, 0, 1_000, false);

      local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("recordTimeSeriesSealedChange".equals(method.getName())) {
              assertThat((byte[]) args[3]).as("the file handed over is the whole sealed file").isNotEmpty();
              handedOver.incrementAndGet();
              return null;
            }
            if ("isReplicated".equals(method.getName()))
              return replicated;
            try {
              return method.invoke(wrapped, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          }));
      shard.compact();
      local.setWrappedDatabaseInstance(wrapped);

      assertThat(shard.getSealedStore().getTotalSampleCount()).isEqualTo(1_000);
      return handedOver.get();
    } finally {
      local.setWrappedDatabaseInstance(wrapped);
      shard.close();
    }
  }

  /**
   * Appends {@code count} samples with timestamps {@code from * 1000 ..} in batches of one transaction each. Shuffled, every batch
   * is written in descending order and the batches alternate between two halves of the range, so the windows of the compaction
   * meet the samples out of order.
   */
  private void append(final TimeSeriesShard shard, final int from, final int count, final boolean shuffled) throws Exception {
    final int batches = (count + BATCH - 1) / BATCH;
    for (int b = 0; b < batches; b++) {
      final int batch = shuffled ? (b % 2 == 0 ? b / 2 : batches - 1 - b / 2) : b;
      final int start = batch * BATCH;
      final int size = Math.min(BATCH, count - start);
      final long[] timestamps = new long[size];
      final Object[] sensors = new Object[size];
      final Object[] values = new Object[size];
      for (int i = 0; i < size; i++) {
        final int n = from + start + (shuffled ? size - 1 - i : i);
        timestamps[i] = n * 1_000L;
        sensors[i] = n % 2 == 0 ? "a" : "b";
        values[i] = (double) n;
      }
      database.begin();
      shard.appendSamples(timestamps, sensors, values);
      database.commit();
    }
  }

  private long mutableSamples(final TimeSeriesShard shard) throws Exception {
    database.begin();
    try {
      return shard.getMutableBucket().getSampleCount();
    } finally {
      database.commit();
    }
  }

  private int mutablePages(final TimeSeriesShard shard) throws Exception {
    database.begin();
    try {
      return shard.getMutableBucket().getDataPageCount();
    } finally {
      database.commit();
    }
  }

  /** Every sample {@code 0 .. total - 1} comes back exactly once, in timestamp order, with its own values. */
  private void assertAllSamplesInOrder(final TimeSeriesShard shard, final int total) throws Exception {
    database.begin();
    try {
      final List<Object[]> rows = shard.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(total);
      for (int n = 0; n < total; n++) {
        final Object[] row = rows.get(n);
        assertThat((long) row[0]).isEqualTo(n * 1_000L);
        assertThat(row[1]).isEqualTo(n % 2 == 0 ? "a" : "b");
        assertThat((double) row[2]).isEqualTo((double) n);
      }
    } finally {
      database.commit();
    }
  }
}
