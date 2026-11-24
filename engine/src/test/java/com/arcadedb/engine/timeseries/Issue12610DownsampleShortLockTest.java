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
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12610: a downsampling pass held the compaction WRITE lock of the shard, and the directory write lock of
 * its sealed store, for the whole of its work - the selection, the decoding and folding of every selected block, the compression of
 * the output and the writing of the new sealed file. Every query and every append of the shard waited for all of it, which on a type
 * with a long history is the first pass after a tier is added.
 * <p>
 * The pass is now split like the merge of small blocks: it folds and writes the temp file against a snapshot of the directory,
 * holding the directory read lock a block at a time and no lock of the shard, and takes the write locks only to install the file. A
 * pass whose directory changed meanwhile is dropped, its temp file deleted. The waits below are hang detectors, not latency bounds:
 * what is asserted is that the query and the append ANSWER while the pass is parked in the middle of its sweep.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12610DownsampleShortLockTest extends TestHelper {

  private static final long MINUTE    = 60_000L;
  private static final int  BLOCKS    = 4;
  private static final int  PER_BLOCK = 120;

  // not the directory of the database TestHelper opens for this class, which is named after the class alone
  private static final String STORE_DIR  = "target/databases/Issue12610DownsampleShortLockTest-store";
  private static final String STORE_PATH = STORE_DIR + "/store";

  /** A store of its own for the tests that drive the two halves of a pass directly: a timestamp and one GORILLA_XOR field. */
  private static final List<ColumnDefinition> STORE_COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  @AfterEach
  void cleanUp() {
    TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = null;
    FileUtils.deleteRecursively(new File(STORE_DIR));
  }

  @Test
  @Timeout(180)
  void queriesAndAppendsOfTheShardAnswerWhileTheDownsampleFoldsTheHistory() throws Exception {
    final TimeSeriesEngine engine = engineWithSealedBlocks("ds_12610_inflight");
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      assertThat(sealed.getBlockCount()).isEqualTo(BLOCKS);

      final CountDownLatch inSweep = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = () -> {
        if (inSweep.getCount() == 0)
          return;
        inSweep.countDown();
        try {
          release.await(120, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      final Future<Object> pass;
      try {
        pass = executor.submit(() -> inContext(() -> {
          engine.applyDownsampling(List.of(new DownsamplingTier(1L, MINUTE)), Long.MAX_VALUE / 2);
          return null;
        }));
        assertThat(inSweep.await(60, TimeUnit.SECONDS)).as("the pass reached the middle of its sweep").isTrue();

        final Future<Integer> query = executor.submit(() -> inContext(() -> rows(engine).size()));
        assertThat(query.get(60, TimeUnit.SECONDS)).as("a query of the shard answers while the pass folds the history")
            .isEqualTo(BLOCKS * PER_BLOCK);

        final Future<Object> append = executor.submit(() -> inContext(() -> {
          database.begin();
          engine.appendSamples(new long[] { Long.MAX_VALUE / 4 }, new Object[] { "a" }, new Object[] { 1.0 });
          database.commit();
          return null;
        }));
        append.get(60, TimeUnit.SECONDS);

        assertThat(pass.isDone()).as("the pass was still parked in its sweep").isFalse();
        assertThat(sealed.getDownsampleRewriteCount()).as("and had installed nothing yet").isZero();
      } finally {
        release.countDown();
      }
      pass.get(60, TimeUnit.SECONDS);

      // one row per minute of the sealed history, plus the sample appended while the pass ran
      assertThat(sealed.getDownsampleRewriteCount()).isEqualTo(1);
      assertThat(rows(engine)).hasSize(BLOCKS * PER_BLOCK / 60 + 1);
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(60, TimeUnit.SECONDS);
      engine.close();
    }
  }

  /**
   * What can still move the directory of a shard between the two halves of a pass is an install of a file shipped by another node,
   * which takes no lock of the shard. The pass is then dropped and nothing is handed to the replication; the next one downsamples
   * the store as it is.
   */
  @Test
  @Timeout(180)
  void aShardPassWhoseStoreWasReplacedWhileItFoldedInstallsNothingAndShipsNothing() throws Exception {
    final TimeSeriesEngine engine = engineWithSealedBlocks("ds_12610_install");
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal wrapped = local.getWrappedDatabaseInstance();
    try {
      final TimeSeriesShard shard = engine.getShard(0);
      final TimeSeriesSealedStore sealed = shard.getSealedStore();
      final AtomicInteger shipped = countShippedSealedFiles(local, wrapped);

      final AtomicInteger hookRuns = new AtomicInteger();
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = () -> {
        if (hookRuns.getAndIncrement() == BLOCKS - 1)
          try {
            // the image of the store as it is, installed as a follower installs the leader's: same blocks, new directory version
            sealed.installSealedFileBytes(sealed.readWholeSealedFile());
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
      };

      assertThat(shard.downsample(Long.MAX_VALUE / 2, MINUTE, 0L, 0, List.of(1), List.of(2))).as("the pass is dropped").isFalse();
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = null;

      assertThat(hookRuns.get()).as("every block was folded before the install was noticed").isEqualTo(BLOCKS);
      assertThat(shipped.get()).as("sealed files handed to the replication").isZero();
      assertThat(sealed.getDownsampleRewriteCount()).isZero();
      assertThat(new File(getDatabasePath(), sealed.getSealedFileName() + ".tmp")).as("the temp file of the dropped pass").doesNotExist();
      assertThat(rows(engine)).as("the store is the one that was installed").hasSize(BLOCKS * PER_BLOCK);

      assertThat(shard.downsample(Long.MAX_VALUE / 2, MINUTE, 0L, 0, List.of(1), List.of(2))).isTrue();
      assertThat(shipped.get()).isEqualTo(1);
      assertThat(rows(engine)).hasSize(BLOCKS * PER_BLOCK / 60);
    } finally {
      local.setWrappedDatabaseInstance(wrapped);
      engine.close();
    }
  }

  @Test
  void aPlanWhoseDirectoryMovedBeforeItsCommitIsDroppedAndLeavesNoTempFile() throws Exception {
    new File(STORE_DIR).mkdirs();
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, STORE_COLUMNS)) {
      appendBlock(store, 0L);
      appendBlock(store, MINUTE);

      final TimeSeriesSealedStore.DownsamplePlan plan = store.prepareDownsample(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1));
      assertThat(plan).isNotNull();
      assertThat(plan.directoryMoved()).isFalse();
      final File temp = new File(STORE_PATH + ".ts.sealed.tmp");
      assertThat(temp).as("the prepare wrote the temp file").exists();
      assertThat(store.getBlockCount()).as("and installed nothing").isEqualTo(2);

      appendBlock(store, 2 * MINUTE);

      assertThat(store.commitDownsample(plan)).as("a plan of a directory that moved is not installed").isFalse();
      assertThat(temp).doesNotExist();
      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(store.getBlockCount()).isEqualTo(3);
      assertThat(store.getTotalSampleCount()).isEqualTo(3L * 60);

      // the next pass folds the store as it is now, the block that landed included
      store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1));
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1);
      assertThat(store.getTotalSampleCount()).isEqualTo(3L * 6);
    }
  }

  /**
   * The prepare notices a rewrite at the next block it reads and gives up there; {@code downsampleBlocks}, which no mutex keeps
   * apart from other writers, then starts over on the live directory, so what it leaves behind is always the downsample of the store.
   */
  @Test
  void aPassThatSeesTheDirectoryMoveWhileItFoldsStartsOverOnTheLiveDirectory() throws Exception {
    new File(STORE_DIR).mkdirs();
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, STORE_COLUMNS)) {
      for (int b = 0; b < 3; b++)
        appendBlock(store, b * MINUTE);

      final AtomicInteger hookRuns = new AtomicInteger();
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = () -> {
        if (hookRuns.getAndIncrement() == 0)
          try {
            appendBlock(store, 3 * MINUTE);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
      };

      final TimeSeriesSealedStore.DownsamplePlan plan = store.prepareDownsample(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1));
      assertThat(plan).isSameAs(TimeSeriesSealedStore.DownsamplePlan.DIRECTORY_MOVED);
      assertThat(new File(STORE_PATH + ".ts.sealed.tmp")).doesNotExist();
      assertThat(store.getDownsampleRewriteCount()).isZero();
      assertThat(store.getBlockCount()).isEqualTo(4);
      assertThat(hookRuns.get()).as("the prepare gave up at the block after the one the rewrite landed behind").isEqualTo(1);

      store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1));
      assertThat(hookRuns.get()).as("one more pass, over the four blocks of the live directory").isEqualTo(1 + 4);
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1);
      assertThat(store.getTotalSampleCount()).as("every block, the one that landed during the first pass included, folded")
          .isEqualTo(4L * 6);
    }
  }

  /** The same retry, inside one call: the first attempt is dropped, the second folds the block that landed during the first. */
  @Test
  void aSingleCallRetriesAPassWhoseDirectoryMovedAndFoldsWhatLanded() throws Exception {
    new File(STORE_DIR).mkdirs();
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, STORE_COLUMNS)) {
      for (int b = 0; b < 3; b++)
        appendBlock(store, b * MINUTE);

      final AtomicInteger hookRuns = new AtomicInteger();
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = () -> {
        if (hookRuns.getAndIncrement() == 0)
          try {
            appendBlock(store, 3 * MINUTE);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
      };

      store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1));

      assertThat(hookRuns.get()).as("one block of the dropped attempt, then the four blocks of the live directory").isEqualTo(1 + 4);
      assertThat(store.getDownsampleRewriteCount()).isEqualTo(1);
      assertThat(store.getTotalSampleCount()).isEqualTo(4L * 6);
      assertThat(new File(STORE_PATH + ".ts.sealed.tmp")).doesNotExist();
    }
  }

  /** A store closed while a pass reads it is not read through its closed channel: the pass is dropped, and the retry refused. */
  @Test
  void aStoreClosedWhileThePassFoldsDropsThePassAndRefusesTheRetry() throws Exception {
    new File(STORE_DIR).mkdirs();
    final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, STORE_COLUMNS);
    for (int b = 0; b < 3; b++)
      appendBlock(store, b * MINUTE);

    TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = () -> {
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = null;
      try {
        store.close();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    };

    assertThat(store.prepareDownsample(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1)))
        .isSameAs(TimeSeriesSealedStore.DownsamplePlan.DIRECTORY_MOVED);
    assertThatThrownBy(() -> store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, List.of(), List.of(1)))
        .isInstanceOf(IOException.class).hasMessageContaining("closed on purpose");
    assertThat(store.getDownsampleRewriteCount()).isZero();
  }

  /**
   * On an HA leader every commit of the database waits for a replication session in progress, so the fold must not run inside one
   * either: the shard opens its session only to install the file and ship it.
   */
  @Test
  @Timeout(180)
  void theFoldRunsOutsideTheReplicationSessionThatShipsTheFile() throws Exception {
    final TimeSeriesEngine engine = engineWithSealedBlocks("ds_12610_session");
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal wrapped = local.getWrappedDatabaseInstance();
    try {
      final AtomicInteger sessions = new AtomicInteger();
      final AtomicInteger openSessions = new AtomicInteger();
      final AtomicInteger shipped = new AtomicInteger();
      local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("recordTimeSeriesSealedChange".equals(method.getName())) {
              assertThat(openSessions.get()).as("the file is shipped inside the session").isEqualTo(1);
              shipped.incrementAndGet();
              return null;
            }
            final boolean session = "runWithCompactionReplication".equals(method.getName());
            if (session) {
              sessions.incrementAndGet();
              openSessions.incrementAndGet();
            }
            try {
              return method.invoke(wrapped, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            } finally {
              if (session)
                openSessions.decrementAndGet();
            }
          }));

      final AtomicInteger foldedInsideASession = new AtomicInteger();
      final AtomicInteger folded = new AtomicInteger();
      TimeSeriesSealedStore.TEST_DOWNSAMPLE_SWEEP_HOOK = () -> {
        folded.incrementAndGet();
        if (openSessions.get() > 0)
          foldedInsideASession.incrementAndGet();
      };

      engine.applyDownsampling(List.of(new DownsamplingTier(1L, MINUTE)), Long.MAX_VALUE / 2);

      assertThat(folded.get()).isEqualTo(BLOCKS);
      assertThat(foldedInsideASession.get()).as("blocks folded while a replication session was open").isZero();
      assertThat(sessions.get()).isEqualTo(1);
      assertThat(shipped.get()).isEqualTo(1);

      // a pass with nothing to fold opens no session at all
      engine.applyDownsampling(List.of(new DownsamplingTier(1L, MINUTE)), Long.MAX_VALUE / 2);
      assertThat(sessions.get()).isEqualTo(1);
    } finally {
      local.setWrappedDatabaseInstance(wrapped);
      engine.close();
    }
  }

  // ---- Helpers ----

  /** An engine of one shard holding {@link #BLOCKS} sealed blocks of {@link #PER_BLOCK} samples one second apart, one series. */
  private TimeSeriesEngine engineWithSealedBlocks(final String name) throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, name, List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)), 1);
    database.commit();
    for (int b = 0; b < BLOCKS; b++) {
      final long[] timestamps = new long[PER_BLOCK];
      final Object[] hosts = new Object[PER_BLOCK];
      final Object[] values = new Object[PER_BLOCK];
      for (int i = 0; i < PER_BLOCK; i++) {
        timestamps[i] = ((long) b * PER_BLOCK + i) * 1_000L;
        hosts[i] = "a";
        values[i] = (double) i;
      }
      database.begin();
      engine.appendSamples(timestamps, hosts, values);
      database.commit();
      database.begin();
      engine.compactAll();
      database.commit();
    }
    return engine;
  }

  private List<Object[]> rows(final TimeSeriesEngine engine) throws Exception {
    database.begin();
    try {
      return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    } finally {
      database.commit();
    }
  }

  private <T> T inContext(final Callable<T> work) throws Exception {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database);
    try {
      return work.call();
    } finally {
      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
    }
  }

  /** Sixty samples one second apart from {@code firstTs}: six of them in every ten-second bucket. */
  private static void appendBlock(final TimeSeriesSealedStore store, final long firstTs) throws IOException {
    final int n = 60;
    final long[] timestamps = new long[n];
    final double[] values = new double[n];
    double sum = 0;
    for (int i = 0; i < n; i++) {
      timestamps[i] = firstTs + i * 1_000L;
      values[i] = i;
      sum += i;
    }
    store.appendBlock(n, timestamps[0], timestamps[n - 1],
        new byte[][] { DeltaOfDeltaCodec.encode(timestamps), GorillaXORCodec.encode(values) },
        new double[] { Double.NaN, 0 }, new double[] { Double.NaN, n - 1 }, new double[] { Double.NaN, sum }, new long[] { 0, n },
        null);
  }

  /** Counts the sealed files the engine hands to the replication; the database answers everything else as it did. */
  private static AtomicInteger countShippedSealedFiles(final LocalDatabase local, final DatabaseInternal wrapped) {
    final AtomicInteger shipped = new AtomicInteger();
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
    return shipped;
  }
}
