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
import com.arcadedb.engine.OwnTransaction;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesEngineTest extends TestHelper {

  @Test
  void multiShardWriteAndQuery() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_engine", cols, 2);

    // Write data — will go to shard based on current thread
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L }, new Object[] { 10.0, 20.0, 30.0 });
    database.commit();

    database.begin();
    final List<Object[]> results = engine.query(1000L, 3000L, null, null);
    assertThat(results).hasSize(3);

    // Results should be sorted by timestamp
    assertThat((long) results.get(0)[0]).isEqualTo(1000L);
    assertThat((long) results.get(1)[0]).isEqualTo(2000L);
    assertThat((long) results.get(2)[0]).isEqualTo(3000L);
    database.commit();

    engine.close();
  }

  @Test
  void shardCount() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_shards", cols, 4);
    assertThat(engine.getShardCount()).isEqualTo(4);
    assertThat(engine.getColumns()).hasSize(2);
    assertThat(engine.getTypeName()).isEqualTo("test_shards");
    database.commit();

    engine.close();
  }

  @Test
  void queryWithTagFilter() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_filter", cols, 1);

    engine.appendSamples(
        new long[] { 1000L, 2000L, 3000L, 4000L },
        new Object[] { "A", "B", "A", "B" },
        new Object[] { 10.0, 20.0, 30.0, 40.0 }
    );
    database.commit();

    database.begin();
    final TagFilter filter = TagFilter.eq(0, "B");
    final List<Object[]> results = engine.query(1000L, 4000L, null, filter);
    assertThat(results).hasSize(2);
    assertThat((String) results.get(0)[1]).isEqualTo("B");
    assertThat((String) results.get(1)[1]).isEqualTo("B");
    database.commit();

    engine.close();
  }

  @Test
  void roundRobinShardDistribution() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_roundrobin", cols, 4);

    // Append 4 batches — round-robin should put one in each shard
    for (int i = 0; i < 4; i++)
      engine.appendSamples(new long[] { (i + 1) * 1000L }, new Object[] { (double) i });

    // Each shard should have exactly 1 sample
    for (int s = 0; s < 4; s++)
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).isEqualTo(1);

    // Append 4 more — second round-robin cycle
    for (int i = 4; i < 8; i++)
      engine.appendSamples(new long[] { (i + 1) * 1000L }, new Object[] { (double) i });

    // Each shard should now have exactly 2 samples
    for (int s = 0; s < 4; s++)
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).isEqualTo(2);

    database.commit();

    // All 8 samples should be queryable
    database.begin();
    final List<Object[]> results = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(results).hasSize(8);
    database.commit();

    engine.close();
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  // #4458
  // Regression test for issue #4458: concurrent appends must not lose samples when they race
  // against compaction Phase 4c on an embedded (standalone) database.
  // <p>
  // NOTE: in standalone mode {@code appendSamples()} holds {@code compactionLock.readLock()} through
  // {@code db.commit()} (the {@code db.isReplicated()} early-release branch is not taken), so the
  // HA-only CME-retry path is NOT exercised here. This test verifies that the standalone locking
  // path remains correct - concurrent appends + compaction lose no data. The actual CME-retry path
  // introduced by the fix is covered by the HA integration test {@code Issue4458TsWalVersionGapIT}.

  // Resets the static fault-injection hooks used by the #4458, #10970 and #9716 tests below.
  @AfterEach
  void clearTestHooks() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = null;
    TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = null;
  }

  // #4458
  @Tag("slow")
  @Test
  void concurrentAppendsRacingPhase4cRetainAllSamples() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");

    // Insert enough initial data to make compaction non-trivial.
    for (int i = 0; i < 300; i++) {
      final long ts = i;
      database.transaction(() ->
          database.command("sql", "INSERT INTO sensor SET ts = ?, v = ?", ts, (double) ts));
    }

    final int appendThreads = 8;
    final int appendsPerThread = 200;
    final int initialCount = 300;
    final int expectedTotal = initialCount + appendThreads * appendsPerThread;

    // Latch: fired when TEST_PRE_PHASE4C_HOOK runs, signalling append threads to start.
    final CountDownLatch hookFired = new CountDownLatch(1);
    // Latch: each append thread counts this down from INSIDE its first transaction (after the
    // append write, before commit), so when the hook proceeds the threads are guaranteed to have
    // an open transaction in flight - not merely scheduled - maximising the append/compaction
    // overlap regardless of CI runner scheduling latency.
    final CountDownLatch appendsInFlight = new CountDownLatch(appendThreads);

    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
      hookFired.countDown();
      // Wait until append threads actually have a transaction in flight, then let Phase 4c run.
      try {
        appendsInFlight.await(10, TimeUnit.SECONDS);
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final List<Throwable> errors = new ArrayList<>();
    final ExecutorService executor = Executors.newFixedThreadPool(appendThreads + 1);

    // Append threads: wait for the hook to fire, then insert as fast as possible.
    for (int t = 0; t < appendThreads; t++) {
      final int ti = t;
      executor.submit(() -> {
        try {
          hookFired.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        final long base = 1_000_000L + (long) ti * 100_000L;
        for (int i = 0; i < appendsPerThread; i++) {
          final long ts = base + i;
          final double val = i;
          final boolean firstInsert = i == 0;
          try {
            database.transaction(() -> {
              database.command("sql", "INSERT INTO sensor SET ts = ?, v = ?", ts, val);
              // Signal in-flight status from within the transaction, before it commits.
              if (firstInsert)
                appendsInFlight.countDown();
            });
          } catch (final Throwable e) {
            synchronized (errors) {
              errors.add(e);
            }
          }
        }
      });
    }

    // Compaction thread: triggers while append threads are racing.
    executor.submit(() -> {
      try {
        ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine().compactAll();
      } catch (final Throwable e) {
        synchronized (errors) {
          errors.add(e);
        }
      }
    });

    executor.shutdown();
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES))
        .as("all tasks complete within timeout").isTrue();

    assertThat(errors)
        .as("no errors during concurrent append+compaction: %s", errors).isEmpty();

    final long actualCount = ((Number) database.command("sql", "SELECT count(*) AS cnt FROM sensor")
        .next().getProperty("cnt")).longValue();
    assertThat(actualCount)
        .as("all %d samples (initial + concurrent) present after compaction race", expectedTotal)
        .isEqualTo(expectedTotal);
  }

  // #4957
  // Regression test for issue #4957: {@link TimeSeriesEngine#appendBatch} dispatched every shard write to the
  // shared {@code shardExecutor} even when the caller had an enclosing transaction open. Each TS-Shard thread
  // then ran the shard's own {@code begin/commit} in its own fresh {@code DatabaseContext}, concurrently with
  // the other shards', so the order in which the shard transactions committed - and, under HA, the order their
  // page versions reached the Raft log - was no longer the calling thread's.
  // <p>
  // (This paragraph used to give the reason as the shard pages being published "out of band with the enclosing
  // commit". Per issue #7410 that happens on both paths: a shard append always commits its own transaction,
  // because an ArcadeDB nested {@code begin/commit} is an independent transaction rather than a savepoint. What
  // the in-thread fallback removes is the cross-thread dispatch, which is what this test asserts and has always
  // asserted - no assertion below changed with that correction.)
  // <p>
  // The HA page-version reordering itself needs a cluster to observe, so this test verifies the fix's
  // observable single-node contract instead: when a transaction is active on the calling thread the shard
  // writes MUST run in-thread (sequential fallback), never on the {@code ArcadeDB-TS-Shard-*} executor
  // threads. The executor's threads are created lazily on first dispatch, so their very existence after the
  // call is a deterministic signal of which path ran.

  private static final int APPEND_BATCH_SHARDS = 2;

  // #4957
  @Test
  void appendBatchInsideEnclosingTransactionRunsInThread() throws Exception {
    final String typeName = "tx_batch";
    final TimeSeriesEngine engine = createBatchEngine(typeName);
    try {
      final int n = 10;
      final long[] timestamps = new long[n];
      final Object[][] columnValues = new Object[1][n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = 1000L + i;
        columnValues[0][i] = (double) i;
      }

      database.begin();
      engine.appendBatch(timestamps, columnValues);
      database.commit();

      // #4957: with an enclosing transaction active, no shard write may be routed to the executor.
      // The fixed-pool threads are created lazily on the first submitted task, so any live
      // ArcadeDB-TS-Shard thread for this type proves the forbidden dispatch happened. (The samples
      // themselves are already committed by now either way - see #7410 - so only the thread names can
      // tell the two paths apart.)
      assertThat(shardThreadsOf(typeName))
          .as("appendBatch inside an enclosing transaction must write the shards on the calling thread")
          .isEmpty();

      database.begin();
      final List<Object[]> rows = engine.query(0L, 10_000L, null, null);
      database.commit();

      assertThat(rows).hasSize(n);
      for (int i = 0; i < n; i++)
        assertThat((long) rows.get(i)[0]).isEqualTo(1000L + i);
    } finally {
      engine.close();
    }
  }

  // #4957
  @Test
  void appendBatchOutsideTransactionStillWritesAllSamplesInParallel() throws Exception {
    final String typeName = "notx_batch";
    final TimeSeriesEngine engine = createBatchEngine(typeName);
    try {
      final int n = 25;
      final long[] timestamps = new long[n];
      final Object[][] columnValues = new Object[1][n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = 1000L + i;
        columnValues[0][i] = (double) i;
      }

      // No enclosing transaction: the parallel per-shard dispatch path stays in effect.
      engine.appendBatch(timestamps, columnValues);

      assertThat(shardThreadsOf(typeName))
          .as("without an enclosing transaction the parallel shard dispatch must be preserved")
          .isNotEmpty();

      database.begin();
      final List<Object[]> rows = engine.query(0L, 10_000L, null, null);
      database.commit();

      assertThat(rows).hasSize(n);
      for (int i = 0; i < n; i++)
        assertThat((long) rows.get(i)[0]).isEqualTo(1000L + i);
    } finally {
      engine.close();
    }
  }

  private TimeSeriesEngine createBatchEngine(final String typeName) throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, typeName, cols, APPEND_BATCH_SHARDS);
    database.commit();
    return engine;
  }

  private static Set<String> shardThreadsOf(final String typeName) {
    final String prefix = "ArcadeDB-TS-Shard-" + typeName + "-";
    return Thread.getAllStackTraces().keySet().stream()
        .map(Thread::getName)
        .filter(name -> name.startsWith(prefix))
        .collect(Collectors.toSet());
  }

  // #7280
  // Issue #7280 - the two engine-side primitives a consistent backup of a TimeSeries database needs.
  // <p>
  // {@link TimeSeriesSealedStore#listSealedFiles(File)} is the enumeration the page-file layer cannot provide,
  // because a sealed store is raw {@code FileChannel} I/O and is never registered with the {@code FileManager}.
  // {@link TimeSeriesCompactionPause} is what stops the enumeration from being a trap: without it a sealed image
  // read after the page image can carry samples the page image still holds in the mutable bucket, which restores
  // as duplicates.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final int  SAMPLES = 30_000;
  private static final long PAUSE_BASE_TS = 1_700_000_000_000L;

  /**
   * A wait that is EXPECTED to expire: it is the assertion that compaction did not get through the pause. Kept
   * short because a stall can only make it more true, never less.
   */
  private static final long BLOCKED_PROBE_MS = 2_000L;

  private void createReadingType() {
    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
  }

  // #7280
  @Test
  void listSealedFilesReturnsTheSealedStoresAndNothingElse() throws Exception {
    createReadingType();
    ingest(SAMPLES);
    engine().compactAll();

    final File dir = new File(database.getDatabasePath());

    // Scratch and staging files sit in the same directory and must never reach an archive: the first is a
    // half-written compaction, the second a half-received HA install.
    assertThat(new File(dir, "Reading_shard_0.ts.sealed.tmp").createNewFile()).isTrue();
    assertThat(new File(dir, "Reading_shard_1.ts.sealed.incoming").createNewFile()).isTrue();

    final List<String> names = Arrays.stream(TimeSeriesSealedStore.listSealedFiles(dir)).map(File::getName).sorted()
        .toList();

    assertThat(names).containsExactly("Reading_shard_0.ts.sealed", "Reading_shard_1.ts.sealed");
    for (final String name : names)
      assertThat(new File(dir, name)).exists().isFile();
  }

  // #7280
  @Test
  void listSealedFilesOnADirectoryWithoutTimeSeriesIsEmptyRatherThanNull() {
    createReadingType();
    assertThat(TimeSeriesSealedStore.listSealedFiles(new File(database.getDatabasePath(), "does-not-exist"))).isEmpty();
  }

  // #7280
  /**
   * The property the backup relies on: while the pause is held no compaction can COMPLETE, so a sealed image
   * read inside it still pairs with the mutable bucket the page image carries.
   */
  @Test
  void aHeldPauseBlocksCompactionUntilItIsReleased() throws Exception {
    createReadingType();
    ingest(SAMPLES);

    final long blocksBefore = totalSealedBlocks();
    assertThat(blocksBefore).as("the test must start with nothing sealed, or it proves nothing").isZero();

    final CountDownLatch compacted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      final Thread compactor = new Thread(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        try {
          engine().compactAll();
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
          compacted.countDown();
        }
      }, "issue7280-compactor");
      compactor.setDaemon(true);
      compactor.start();

      assertThat(compacted.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("compaction must not complete while the pause is held").isFalse();
      assertThat(totalSealedBlocks()).as("no sealed block may appear while the pause is held").isZero();

      pause.close();

      assertThat(compacted.await(60, TimeUnit.SECONDS)).as("compaction must complete once the pause is released")
          .isTrue();
      assertThat(failure.get()).isNull();
      compactor.join(60_000);
    }

    assertThat(totalSealedBlocks()).as("the released compaction must actually have sealed something")
        .isGreaterThan(blocksBefore);
  }

  // #7280
  /** Closing twice is a no-op: the backup releases the pause early and the try-with-resources closes it again. */
  @Test
  void closingThePauseTwiceIsANoOp() throws Exception {
    createReadingType();
    ingest(1_000);

    final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L);
    pause.close();
    pause.close();

    // A double release would have thrown above on an unheld lock; compaction still works afterwards.
    engine().compactAll();
    assertThat(totalSealedBlocks()).isGreaterThan(0);
  }

  // #7280
  /**
   * The pause covers every shard of every TimeSeries type and nothing else, so a database with no TimeSeries
   * type pays nothing for it and one with several is covered whole.
   */
  @Test
  void thePauseCoversEveryShardOfEveryTimeSeriesTypeAndNothingElse() {
    createReadingType();
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      assertThat(pause.getPausedShards()).isEqualTo(engine().getShardCount());
    }

    database.command("sql", "CREATE TIMESERIES TYPE Second TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 3");
    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      assertThat(pause.getPausedShards()).isEqualTo(engine().getShardCount() + 3);
    }
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine();
  }

  private long totalSealedBlocks() {
    long total = 0;
    for (int i = 0; i < engine().getShardCount(); i++)
      total += engine().getShard(i).getSealedStore().getBlockCount();
    return total;
  }

  private void ingest(final int samples) throws Exception {
    final long[] timestamps = new long[samples];
    final Object[] hosts = new Object[samples];
    final Object[] values = new Object[samples];
    for (int i = 0; i < samples; i++) {
      timestamps[i] = PAUSE_BASE_TS + i * 1_000L;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    engine().appendBatch(timestamps, new Object[][] { hosts, values });
  }

  // #7410
  // Regression test for issue #7410: {@link TimeSeriesEngine#appendSamples} documented that, inside an enclosing
  // transaction, "the shard's internal begin/commit nests into that transaction, so the mutable-bucket page writes
  // are published and replicated by the enclosing commit as a single, in-order transaction". The code never did
  // that: {@code TimeSeriesShard.appendSamples} commits its own transaction whatever the caller has open, for the
  // reason its javadoc now gives in full - an ArcadeDB nested {@code begin/commit} is an independent transaction
  // rather than a savepoint.
  // <p>
  // These are characterization tests: they pin the behaviour the corrected javadoc now states, one per entry point
  // that reaches the shard append. They fail against the <i>old documented</i> contract, not against the code - the
  // defect #7410 reports is exactly that the two disagreed. A rollback of the enclosing transaction is the probe,
  // because it is the one observation that separates "published by the enclosing commit" from "already committed":
  // only the first can be undone.
  // <p>
  // The DOCUMENT row written in the same transaction is the control in {@link #sqlInsertIntoTimeSeriesTypeSurvivesTheRollback()}:
  // it proves the statements ran inside a live transaction that really did roll back, so "the sample survived"
  // cannot be explained by the transaction never having existed.
  // <p>
  // {@code Issue7370GrpcTimeSeriesInTransactionIT.timeSeriesAppendsAreNotPartOfTheEnclosingTransaction} pins the
  // same contract over the wire; this class pins it embedded, where the engine and SQL entry points live.

  private static final int SCOPE_SHARDS = 2;

  /**
   * Written into the same transaction as every append below and never into any other. Its fate after the
   * rollback is the control: it proves the transaction was open, really contained the statements, and really
   * rolled back - so "the sample survived" cannot be explained by the transaction never having existed.
   */
  private void createWitnessType() {
    database.command("sql", "CREATE DOCUMENT TYPE Witness");
  }

  // #7410
  /**
   * Entry point 1: {@link TimeSeriesEngine#appendSamples(long[], Object[][])}, which delegates to the
   * {@link TimeSeriesRowSource} overload and from there to a single shard.
   */
  @Test
  void appendSamplesSurvivesTheRollbackOfTheEnclosingTransaction() throws Exception {
    createWitnessType();
    final TimeSeriesEngine engine = createScopeEngine("append_rollback");
    try {
      database.begin();
      database.command("sql", "INSERT INTO Witness SET name = 'w'");
      engine.appendSamples(new long[] { 1_000L }, new Object[][] { { 11.0 } });
      assertThat(countOfWitnesses())
          .as("the control row is visible inside its own open transaction")
          .isEqualTo(1L);
      database.rollback();

      assertThat(countOfWitnesses())
          .as("the rollback did take the DOCUMENT row, so the transaction was real")
          .isZero();
      assertThat(scopeTimestampsOf(engine))
          .as("#7410: appendSamples commits its own shard transaction, so the enclosing rollback cannot undo it")
          .containsExactly(1_000L);
    } finally {
      engine.close();
    }
  }

  // #7410
  /**
   * Entry point 2: {@link TimeSeriesEngine#appendBatch(long[], Object[][])}, whose in-thread fallback under an
   * enclosing transaction (issue #4957) does not change the transaction scope of the shard writes it makes -
   * it only keeps them off the shard executor.
   */
  @Test
  void appendBatchSurvivesTheRollbackOfTheEnclosingTransaction() throws Exception {
    createWitnessType();
    final TimeSeriesEngine engine = createScopeEngine("batch_rollback");
    try {
      final int n = 6;
      final long[] timestamps = new long[n];
      final Object[][] columnValues = new Object[1][n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = 2_000L + i;
        columnValues[0][i] = (double) i;
      }

      database.begin();
      database.command("sql", "INSERT INTO Witness SET name = 'w'");
      engine.appendBatch(timestamps, columnValues);
      assertThat(countOfWitnesses())
          .as("the control row is visible inside its own open transaction")
          .isEqualTo(1L);
      database.rollback();

      assertThat(countOfWitnesses())
          .as("the rollback did take the DOCUMENT row, so the transaction was real")
          .isZero();
      assertThat(scopeTimestampsOf(engine))
          .as("#7410: every shard sub-batch committed its own transaction before the enclosing rollback ran")
          .containsExactly(2_000L, 2_001L, 2_002L, 2_003L, 2_004L, 2_005L);
    } finally {
      engine.close();
    }
  }

  // #7410
  /**
   * Entry point 3: {@code SaveElementStep.saveToTimeSeries}, reached by {@code INSERT INTO <timeseries type>}.
   * This is the path a SQL user meets, and the one where the divergence is user-visible: the INSERT is not
   * atomic with the rest of its own transaction.
   */
  @Test
  void sqlInsertIntoTimeSeriesTypeSurvivesTheRollback() {
    createWitnessType();
    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts FIELDS (value DOUBLE)");

    database.begin();
    database.command("sql", "INSERT INTO Witness SET name = 'w'");
    database.command("sql", "INSERT INTO Reading SET ts = 3000, value = 30.0");
    assertThat(countOfWitnesses())
        .as("the control row is visible inside its own open transaction")
        .isEqualTo(1L);
    database.rollback();

    assertThat(countOfWitnesses())
        .as("the rollback did take the DOCUMENT row, so the transaction was real")
        .isZero();
    assertThat(countOfReadings())
        .as("#7410: INSERT INTO a TIMESERIES type is not atomic with the transaction that contains it")
        .isEqualTo(1L);
  }

  /** Counts the control rows, from outside any transaction of this test's making. */
  private long countOfWitnesses() {
    return rowCountOf("Witness");
  }

  /** Counts the samples of the SQL-created series, through the SQL reader a user would use. */
  private long countOfReadings() {
    return rowCountOf("Reading");
  }

  /**
   * Counts the rows of a type by walking them.
   * <p>
   * Not {@code SELECT count(*)}: per {@code engine/CLAUDE.md} that form routes to {@code CountFromTypeStep}
   * and sums {@code LocalBucket.count()}, which is a cached counter plus the transaction delta rather than a
   * scan - "the wrong tool when you are verifying correctness in a test" (code review of this PR). Walking
   * the rows is ground truth for both a DOCUMENT type and a TIMESERIES one.
   */
  private long rowCountOf(final String typeName) {
    long count = 0;
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  /**
   * Every sample timestamp the engine holds, merge-sorted across shards, over a range wider than any test
   * writes.
   * <p>
   * The read is wrapped in a transaction of its own, as {@code TimeSeriesEngineTest} does.
   * It happens to work unwrapped after a {@code rollback()} - {@code popIfNotLastTransaction()} leaves the
   * sole {@code TransactionContext} on the thread's stack rather than popping it, and neither
   * {@code getTransaction()} nor {@code TransactionContext.getPage()} gates on {@code isActive()}, so the
   * read falls through to the {@code PageManager} - but that is an internal detail and not a contract these
   * tests should depend on for their correctness (code review of this PR).
   */
  private List<Long> scopeTimestampsOf(final TimeSeriesEngine engine) throws Exception {
    database.begin();
    try {
      return engine.query(0L, 1_000_000L, null, null).stream().map(row -> (Long) row[0]).toList();
    } finally {
      database.commit();
    }
  }

  /**
   * Builds a two-shard engine directly, as {@code TimeSeriesEngineTest} does. The constructor
   * initialises each shard's header page in a transaction of its own, so it is wrapped in one here.
   */
  private TimeSeriesEngine createScopeEngine(final String typeName) throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, typeName, cols, SCOPE_SHARDS);
    database.commit();
    return engine;
  }

  // #7657
  // The measurement that decided issue #7657, and the pin that keeps the decision from being undone by accident.
  // <p>
  // #7410 established that a TimeSeries append commits its own transaction whatever the caller has open, and
  // corrected the javadoc that said otherwise ({@code TimeSeriesEngineTest} pins that). #7657 then
  // asked the deferred question: should the append instead <i>join</i> the enclosing transaction, so that
  // {@code INSERT INTO <timeseries type>} becomes atomic with its own statement's transaction? The answer recorded
  // on {@code TimeSeriesShard.appendSamples} is no, and these two tests are why - together they are an experiment
  // rather than an appeal to the surrounding comments.
  // <p>
  // {@link #concurrentAppendsToOneShardFromOpenTransactionsAllSucceed()} measures the property the current design
  // delivers. {@link #twoTransactionsHoldingTheSameHeaderPageCannotBothCommit()} measures what that property rests
  // on, by reproducing the shape the alternative would have: it drives the mutable bucket from two transactions the
  // shard does not own and shows that the second commit cannot land. Every append writes page 0 - the header
  // carries the sample count and the min/max timestamps - so two appends to one shard that are staged in separate
  // transactions and committed by their callers always contend for the same page version.
  // <p>
  // Today they never are staged that way: {@code TimeSeriesShard.appendLock} is held across the shard's whole
  // {@code begin}/{@code commit} cycle, so the two appends are serialized and the second reads the version the
  // first published. An append that joined the caller's transaction could not be serialized that way - the commit
  // would belong to the caller, and holding a shard lock until an arbitrary user transaction ends is not something
  // the shard can do - which is what turns the first test's outcome into the second's.

  /** One shard, so every append in this class contends for the same page 0 without depending on round-robin. */
  private static final int SINGLE_SHARD = 1;

  private static final int THREADS            = 4;
  private static final int APPENDS_PER_THREAD = 5;

  // #7657
  /**
   * Concurrent ingest into one shard, every append made from a thread that already has a transaction open.
   * <p>
   * This is the workload the self-committing design is for, and the one the alternative would put at risk: all
   * {@code THREADS * APPENDS_PER_THREAD} appends succeed, none of them raises, and all of the samples are
   * readable afterwards even though every calling transaction was rolled back. The rollback is deliberate - it
   * makes the test say both things at once, that the appends did not conflict and that they were never part of
   * the transactions they were made from.
   */
  @Test
  void concurrentAppendsToOneShardFromOpenTransactionsAllSucceed() throws Exception {
    final TimeSeriesEngine engine = createSingleShardEngine("concurrent_append");
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done  = new CountDownLatch(THREADS);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      for (int t = 0; t < THREADS; t++) {
        final int threadIndex = t;
        final Thread worker = new Thread(() -> {
          try {
            start.await();
            database.begin();
            for (int i = 0; i < APPENDS_PER_THREAD; i++)
              engine.appendSamples(new long[] { timestampFor(threadIndex, i) }, new Object[][] { { (double) i } });
            // Rolling back proves in the same breath that the samples below owe nothing to this transaction.
            database.rollback();
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "issue7657-append-" + t);
        worker.setDaemon(true);
        worker.start();
      }

      start.countDown();
      assertThat(done.await(60, TimeUnit.SECONDS))
          .as("every appending thread finished")
          .isTrue();

      assertThat(failure.get())
          .as("concurrent appends to a single shard all succeed: no caller is handed a page-0 conflict to "
              + "deal with. (That they never even contend is the shard's doing, not this assertion's - a "
              + "conflict absorbed by appendSamples' own retry loop would look the same from here. "
              + "twoTransactionsHoldingTheSameHeaderPageCannotBothCommit is where the mechanism is measured.)")
          .isNull();

      assertThat(singleShardTimestampsOf(engine))
          .as("#7657: every append committed itself, so all of them survive the rollback of the transaction "
              + "they were made from")
          .containsExactlyInAnyOrderElementsOf(expectedTimestamps());
    } finally {
      engine.close();
    }
  }

  // #7657
  /**
   * The shape an append that joined the enclosing transaction would have, and the reason #7657 was decided
   * against it.
   * <p>
   * Both threads stage a modification of the same shard's page 0 in a transaction the shard does not own, and
   * only then does either commit - exactly what would happen if {@code TimeSeriesShard.appendSamples} wrote
   * through the caller's transaction and returned, leaving the commit to the caller. The second commit raises
   * {@link ConcurrentModificationException}, and there is nowhere left to absorb it: the shard's three-attempt
   * retry loop retries its <i>own</i> commit, and a commit that belongs to the caller could only be retried by
   * replaying the caller's whole transaction.
   * <p>
   * Written as an assertion about the pair rather than about a particular thread, because which of the two
   * commits lands first is a race - what is not a race is that exactly one of them can.
   */
  @Test
  void twoTransactionsHoldingTheSameHeaderPageCannotBothCommit() throws Exception {
    final TimeSeriesEngine engine = createSingleShardEngine("joined_tx_shape");
    try {
      final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();

      final CountDownLatch staged = new CountDownLatch(2);
      final CountDownLatch done   = new CountDownLatch(2);
      final AtomicInteger  commits = new AtomicInteger();
      final AtomicInteger  conflicts = new AtomicInteger();
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();

      for (int t = 0; t < 2; t++) {
        final long timestamp = 90_000L + t;
        final Thread worker = new Thread(() -> {
          try {
            database.begin();
            // Stage the page-0 write in THIS thread's transaction, and do not commit it here. That is the
            // whole of the difference: the write is made, the publication is somebody else's business.
            bucket.appendSamples(bucket.newRowSource(new long[] { timestamp }, new Object[][] { { 1.0 } }));
            staged.countDown();
            if (!staged.await(60, TimeUnit.SECONDS))
              throw new IllegalStateException("the other transaction never staged its write");
            try {
              database.commit();
              commits.incrementAndGet();
            } catch (final ConcurrentModificationException e) {
              conflicts.incrementAndGet();
              if (database.isTransactionActive())
                database.rollback();
            }
          } catch (final Throwable e) {
            unexpected.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "issue7657-joined-" + t);
        worker.setDaemon(true);
        worker.start();
      }

      assertThat(done.await(60, TimeUnit.SECONDS))
          .as("both transactions finished")
          .isTrue();
      assertThat(unexpected.get())
          .as("the only failure this test expects is the page-version conflict it is measuring")
          .isNull();

      assertThat(commits.get())
          .as("exactly one of the two transactions can publish its version of page 0. A 2 here means the two "
              + "stopped contending - something serialized them, and the premise of the #7657 decision would "
              + "need re-checking; a 0 means neither could commit, which is a different bug entirely")
          .isEqualTo(1);
      assertThat(conflicts.get())
          .as("#7657: the other loses its whole transaction to a ConcurrentModificationException. An append "
              + "that joined the caller's transaction would hand this failure to the caller, where the shard's "
              + "own retry loop cannot reach it. A 0 here is the same finding as a 2 above, seen from the "
              + "other side")
          .isEqualTo(1);
    } finally {
      engine.close();
    }
  }

  /** Distinct per (thread, append) so a lost or duplicated append shows up as a missing or extra timestamp. */
  private static long timestampFor(final int threadIndex, final int appendIndex) {
    return 10_000L + threadIndex * 1_000L + appendIndex;
  }

  private static List<Long> expectedTimestamps() {
    final List<Long> expected = new ArrayList<>(THREADS * APPENDS_PER_THREAD);
    for (int t = 0; t < THREADS; t++)
      for (int i = 0; i < APPENDS_PER_THREAD; i++)
        expected.add(timestampFor(t, i));
    return expected;
  }

  /**
   * Every sample timestamp the engine holds, over a range wider than any test here writes. Wrapped in a
   * transaction of its own for the reason {@code TimeSeriesEngineTest.singleShardTimestampsOf} records:
   * reading straight after a {@code rollback()} happens to work, but only through an internal detail of how
   * {@code popIfNotLastTransaction()} retains the sole transaction context.
   */
  private List<Long> singleShardTimestampsOf(final TimeSeriesEngine engine) throws Exception {
    database.begin();
    try {
      return engine.query(0L, 1_000_000L, null, null).stream().map(row -> (Long) row[0]).toList();
    } finally {
      database.commit();
    }
  }

  /**
   * Builds a single-shard engine directly, as {@code TimeSeriesEngineTest} does. The constructor
   * initialises the shard's header page in a transaction of its own, so it is wrapped in one here.
   */
  private TimeSeriesEngine createSingleShardEngine(final String typeName) throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, typeName, cols, SINGLE_SHARD);
    database.commit();
    return engine;
  }

  // #7732
  // Issue #7732: when a TimeSeries append's OWN commit failed, the error handling rolled back the CALLER's
  // transaction.
  // <p>
  // #7410 pinned the other direction of the same relationship - the append commits its own transaction, so the
  // caller's {@code rollback()} does not take the samples back. This is the direction nothing pinned. A failed
  // {@code commit()} has already popped the transaction it failed to commit ({@code LocalDatabase.commit()} does it
  // in a {@code finally}, and {@code DatabaseContext.popIfNotLastTransaction()} removes a nested context whenever
  // the stack holds more than one), so {@code db.isTransactionActive()} in the catch answered for the CALLER's
  // transaction, and {@code db.rollback()} discarded the caller's own uncommitted work. On the retry arm the append
  // then succeeded and returned normally, so the caller got no signal at all: the rows it had written in the same
  // transaction were simply gone.
  // <p>
  // The single-transaction case was safe by accident - with nothing underneath, the failed transaction stays on the
  // stack and is already inactive - which is why only a caller who had a transaction open was bitten.
  // <p>
  // The probe is a commit injected to fail, because that is the only observation that separates "rolled back its
  // own" from "rolled back mine": the caller's document row is written in the same transaction and its survival is
  // what the append must not decide.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7732">issue #7732</a>
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long RETRY_BASE_TS = 1_700_000_000_000L;

  /**
   * Fails the next {@code commit()} that reaches the wrapped database, whatever calls it, and delegates
   * everything else. Installed as the database's wrapped instance, which is what {@code TimeSeriesShard}
   * begins and commits its append transaction on ({@code getWrappedDatabaseInstance()}); on a standalone
   * database that is normally the database itself, and under HA it is the Raft wrapper, so this stands exactly
   * where a quorum loss or a step-down would.
   */
  private static final class FailingCommits implements InvocationHandler {
    private final DatabaseInternal delegate;
    private final AtomicInteger    failuresLeft;
    private final Class<? extends RuntimeException> failWith;

    private FailingCommits(final DatabaseInternal delegate, final int failures,
        final Class<? extends RuntimeException> failWith) {
      this.delegate = delegate;
      this.failuresLeft = new AtomicInteger(failures);
      this.failWith = failWith;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
      if ("commit".equals(method.getName()) && method.getParameterCount() == 0
          && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        // Rolled back and popped, which is the state a real failed commit leaves: commit1stPhase has already
        // rolled the transaction back on every failure arm, and commit()'s own finally pops it. That is the
        // state the bug needed - the append's context gone, the CALLER's back on top of the stack and active -
        // and it is why nothing of this transaction reaches the pages either.
        delegate.rollback();
        throw failWith == ConcurrentModificationException.class
            ? new ConcurrentModificationException("injected commit failure")
            : new RuntimeException("injected commit failure");
      }
      return method.invoke(delegate, args);
    }
  }

  private void installFailingCommits(final int failures, final Class<? extends RuntimeException> failWith) {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    local.setWrappedDatabaseInstance(failingCommits(local.getWrappedDatabaseInstance(), failures, failWith));
  }

  private static DatabaseInternal failingCommits(final DatabaseInternal delegate, final int failures,
      final Class<? extends RuntimeException> failWith) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, new FailingCommits(delegate, failures, failWith));
  }

  private TimeSeriesEngine createAll() {
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);
    database.command("sql", "CREATE TIMESERIES TYPE Point TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
    return ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();
  }

  private long controlRows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Control")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  // #7732
  /**
   * The reported shape: the append's commit hits an MVCC conflict, the CME arm retries and succeeds, and the
   * caller is never told anything went wrong - so the caller's own row had better still be there to commit.
   */
  @Test
  void aRetriedAppendLeavesTheCallerTransactionIntact() throws Exception {
    final TimeSeriesEngine engine = createAll();
    installFailingCommits(1, ConcurrentModificationException.class);

    database.begin();
    database.newDocument("Control").set("k", 1).save();

    engine.appendSamples(new long[] { RETRY_BASE_TS }, new Object[][] { new Object[] { 1.0d } });

    assertThat(database.isTransactionActive())
        .as("the append's failed commit must not have taken the caller's transaction with it")
        .isTrue();
    database.commit();

    assertThat(controlRows()).as("the caller's own row survived the append's internal retry").isEqualTo(1);

    database.begin();
    try {
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
          .as("and the retry appended the sample once, not twice")
          .hasSize(1);
    } finally {
      database.commit();
    }
  }

  // #7732
  /**
   * The same when the append gives up: it throws, which is the caller's signal, and the caller is still the one
   * who decides what happens to its transaction.
   */
  @Test
  void anAppendThatFailsForGoodStillLeavesTheDecisionToTheCaller() throws Exception {
    final TimeSeriesEngine engine = createAll();
    // One failure is all it takes on an arm that does not retry.
    installFailingCommits(1, RuntimeException.class);

    database.begin();
    database.newDocument("Control").set("k", 1).save();

    assertThatThrownBy(() -> engine.appendSamples(new long[] { RETRY_BASE_TS }, new Object[][] { new Object[] { 1.0d } }))
        .as("a failure the append cannot retry is reported, not swallowed")
        .isInstanceOf(Exception.class);

    assertThat(database.isTransactionActive()).isTrue();
    database.commit();

    assertThat(controlRows()).as("the caller's row was never the append's to discard").isEqualTo(1);
  }

  // #7732
  /** With no caller transaction there is nothing underneath to protect, and the retry still works. */
  @Test
  void anAppendWithNoCallerTransactionIsUnaffected() throws Exception {
    final TimeSeriesEngine engine = createAll();
    installFailingCommits(1, ConcurrentModificationException.class);

    engine.appendSamples(new long[] { RETRY_BASE_TS }, new Object[][] { new Object[] { 1.0d } });

    database.begin();
    try {
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(1);
    } finally {
      database.commit();
    }
  }

  // #7732
  /**
   * The third site in {@code TimeSeriesShard}'s constructor: crash recovery, which runs on every open of a shard
   * whose bucket was left mid-compaction (code review on PR #7747). Unlike the two above it commits on
   * {@code database} rather than on the wrapped instance - a local repair must not be replicated - so the failure
   * is injected by handing the ENGINE a database whose commit fails, which is where that call resolves from.
   * <p>
   * The message assertion is what makes the injection exact: it is the crash-recovery commit that failed and not
   * some earlier one, or this test would prove nothing about this block.
   */
  @Test
  void aFailedCrashRecoveryCommitLeavesTheCallerTransactionIntact() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "Recovered", cols, 1);
    database.commit();

    // The state a crash mid-compaction leaves behind, which is what makes the next open run crash recovery.
    database.begin();
    engine.getShard(0).getMutableBucket().setCompactionInProgress(true);
    database.commit();

    database.begin();
    database.newDocument("Control").set("k", 1).save();

    // A second engine over the same files, as a retried initEngine() builds one: the components are registered,
    // so nothing but the recovery block commits on the way through.
    assertThatThrownBy(() -> new TimeSeriesEngine(
        failingCommits((DatabaseInternal) database, 1, RuntimeException.class), "Recovered", cols, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Crash recovery failed for shard");

    assertThat(database.isTransactionActive())
        .as("the recovery's failed commit must not have taken the caller's transaction with it")
        .isTrue();
    database.commit();

    assertThat(controlRows()).isEqualTo(1);
  }

  // #7732
  /** {@code OwnTransaction} itself: the rollback is a no-op once the commit has taken its transaction away. */
  @Test
  void theOwnTransactionHelperNeverRollsBackSomebodyElsesTransaction() {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);

    db.begin();
    db.newDocument("Control").set("k", 1).save();

    final OwnTransaction nested = OwnTransaction.begin(db);
    nested.commit();
    nested.rollbackIfMine();

    assertThat(db.isTransactionActive()).isTrue();
    db.commit();
    assertThat(controlRows()).isEqualTo(1);
  }

  // #7732
  /** And it DOES roll back its own while that one is still there, so nothing half-written is published. */
  @Test
  void theOwnTransactionHelperStillRollsBackItsOwn() {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);

    db.begin();
    db.newDocument("Control").set("k", 1).save();

    final OwnTransaction nested = OwnTransaction.begin(db);
    db.newDocument("Control").set("k", 2).save();
    nested.rollbackIfMine();

    assertThat(db.isTransactionActive()).as("the caller's transaction is still open underneath").isTrue();
    db.commit();
    assertThat(controlRows()).as("the nested row was rolled back, the caller's was not").isEqualTo(1);
  }

  // #10970
  // Regression test for issue #10970: an append whose commit keeps losing a conflict (for example a Raft refusal because the
  // transaction was prepared before a schema change, which tells the caller to retry) used to leave
  // {@link TimeSeriesShard#appendSamples} wrapped in a plain {@code IOException} once its own retries were spent. A caller keying
  // on {@link NeedRetryException} - the HTTP layer answering 503 - never saw it, so the client did not retry and the points were
  // lost.

  // #10970
  @Test
  void exhaustedConflictRetriesSurfaceAsRetryable() {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();

    final AtomicInteger attempts = new AtomicInteger();
    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = () -> {
      attempts.incrementAndGet();
      throw new ConcurrentModificationException("prepared before the schema change. Please retry the operation");
    };

    assertThatThrownBy(() -> engine.appendSamples(new long[] { 1L, 2L }, new Object[] { 1.0, 2.0 }))
        .isInstanceOf(NeedRetryException.class);
    assertThat(attempts.get()).as("the append retried before giving up").isEqualTo(3);
  }

  // #10970
  @Test
  void conflictClearingWithinTheRetriesLosesNothing() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();

    final AtomicInteger attempts = new AtomicInteger();
    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = () -> {
      if (attempts.incrementAndGet() < 3)
        throw new ConcurrentModificationException("transient conflict");
    };

    engine.appendSamples(new long[] { 1L, 2L }, new Object[] { 1.0, 2.0 });

    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = null;
    final long count = ((Number) database.query("sql", "SELECT count(*) AS cnt FROM sensor").next().getProperty("cnt")).longValue();
    assertThat(count).isEqualTo(2L);
    assertThat(attempts.get()).as("the third attempt committed").isEqualTo(3);
  }

  // #6839
  // Issue #6839: {@code initEngine()} has to be retryable, because since #6356 it is the ONLY way a type registered
  // without a storage engine ever gets one - the HA sealed-blob repair path drives exactly this call.
  // <p>
  // What stood in the way was a cleanup that looked like hygiene. Three places closed the shard's mutable bucket
  // when something else failed to open: {@code TimeSeriesShard}'s sealed-store construction, its crash-recovery
  // block, and {@code TimeSeriesEngine}'s unwind of the shards it had already built. The bucket is registered with
  // the schema on every branch that reaches them, so the schema owns closing it and none of those closes ever
  // prevented a leak - but {@code PaginatedComponentFile.close()} sets {@code open=false} and its own lazy reopen
  // then refuses ("was closed on purpose") for the life of the process. So the failed attempt destroyed the thing
  // the next attempt needs, and the retry came back reporting {@code isEngineAvailable() == true} over a component
  // that throws on every page it touches. Harmless while a failed {@code initEngine()} meant the type vanished from
  // the schema; the whole obstacle to recovery once #6356 made it stay.
  // <p>
  // This class pins the CRASH-RECOVERY door, the one a repair is most likely to meet: a {@code .ts.sealed} is most
  // often damaged by a crash during compaction, which is precisely when {@code isCompactionInProgress()} is true on
  // reopen, so the retry that installs a good sealed file runs straight into this block. The sibling doors are
  // pinned by {@code Issue6839TsSealedBlobRecoveryTest} (sealed-store construction, and the multi-shard unwind).
  // <p>
  // The failure is injected with a BROKEN SYMLINK at the {@code .ts.sealed.tmp} path {@code truncateToBlockCount}
  // writes through: {@code File.exists()} follows the link and answers false, so {@code TimeSeriesSealedStore}'s
  // constructor skips its own stale-tmp cleanup and opens normally, and then
  // {@code new RandomAccessFile(tempPath, "rw")} inside the rewrite resolves the link to a directory that is not
  // there and throws. Deterministic, and it needs no test hook in production code. A plain directory does not work
  // for this: the constructor deletes an empty one before the shard ever reaches crash recovery, and throws on a
  // non-empty one at construction, which is the OTHER door and is already covered.
  // <p>
  // Note also that a watermark merely disagreeing with the block count does NOT throw - {@code truncateToBlockCount}
  // returns early when the target is at or above the directory size - so the watermark below is deliberately BELOW
  // the block count, to get past that guard and reach the rewrite.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final String TYPE_NAME = "Cpu";
  private static final int    CRASH_RECOVERY_ROWS      = 3_000;

  // #6839
  @Test
  void initEngineRecoversAfterCrashRecoveryFailedOnAPreviousAttempt() throws Exception {
    createTypeAndCompact();

    // A crash mid-compaction, in the state it leaves behind: the flag set, and a watermark BELOW the sealed
    // store's block count so recovery has real truncation work to do rather than returning early.
    final long blocks = engineOf().getShard(0).getSealedStore().getBlockCount();
    assertThat(blocks).isGreaterThan(0L);
    database.transaction(() -> {
      try {
        engineOf().getShard(0).getMutableBucket().setCompactionInProgress(true);
        engineOf().getShard(0).getMutableBucket().setCompactionWatermark(blocks - 1);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    final Path blocker = new File(getDatabasePath(), TYPE_NAME + "_shard_0.ts.sealed.tmp").toPath();
    database.close();
    try {
      createBrokenSymlink(blocker);
    } finally {
      database = factory.open();
    }

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(broken.isEngineAvailable()).as("crash recovery must have failed, or this test proves nothing")
        .isFalse();

    // The repair: whatever was blocking the rewrite is gone. This stands in for the HA path installing a good
    // sealed file - what matters here is only that the SECOND initEngine() has a reason to succeed.
    Files.delete(blocker);

    broken.initEngine();

    assertThat(broken.isEngineAvailable()).as("the retry must be able to succeed").isTrue();
    assertThat(broken.getEngineUnavailableReason()).isNull();

    // The point of the whole exercise: the mutable bucket the failed attempt used to close is still usable. The
    // flag above flips to true either way - only touching the file distinguishes a real recovery from a component
    // that will throw on the first page it reads. Asserted as a DELTA rather than a total, because the recovery
    // this test provoked legitimately truncates the sealed store to the watermark and how many samples that
    // discards is beside the point: what has to hold is that the bucket still reads and still takes writes.
    final long afterRecovery = countSamples();
    appendRows(broken.getEngine(), CRASH_RECOVERY_ROWS, 100);
    assertThat(countSamples()).isEqualTo(afterRecovery + 100L);
  }

  // #6839
  /** With nothing blocking it, the same crash-recovery state simply recovers on the first open, as it always did. */
  @Test
  void anUnobstructedCrashRecoveryStillRunsOnTheFirstOpen() throws Exception {
    createTypeAndCompact();

    final long blocks = engineOf().getShard(0).getSealedStore().getBlockCount();
    database.transaction(() -> {
      try {
        engineOf().getShard(0).getMutableBucket().setCompactionInProgress(true);
        engineOf().getShard(0).getMutableBucket().setCompactionWatermark(blocks - 1);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    database.close();
    database = factory.open();

    final LocalTimeSeriesType reopened = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    assertThat(reopened.isEngineAvailable()).isTrue();
    assertThat(reopened.getEngine().getShard(0).getMutableBucket().isCompactionInProgress()).isFalse();
    assertThat(reopened.getEngine().getShard(0).getSealedStore().getBlockCount()).isEqualTo(blocks - 1);
  }

  // #6839
  /** A second {@code initEngine()} on a healthy type is a no-op, not a second engine over the same files. */
  @Test
  void initEngineOnAHealthyTypeIsANoOp() throws Exception {
    createTypeAndCompact();

    final LocalTimeSeriesType healthy = (LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME);
    final TimeSeriesEngine before = healthy.getEngine();

    healthy.initEngine();

    assertThat(healthy.getEngine()).isSameAs(before);
    assertThat(countSamples()).isEqualTo(CRASH_RECOVERY_ROWS);
  }

  // ---- Helpers ----

  /**
   * Skips the test rather than failing it where symbolic links cannot be created (Windows without the privilege).
   * The production fix is platform-independent; only this injection is not.
   */
  private static void createBrokenSymlink(final Path link) {
    try {
      Files.createSymbolicLink(link, link.resolveSibling("no-such-directory-6839").resolve("target"));
    } catch (final IOException | UnsupportedOperationException e) {
      assumeTrue(false, "symbolic links are not available here: " + e.getMessage());
    }
    assertThat(Files.exists(link)).as("a BROKEN link, so the sealed store's stale-tmp cleanup skips it").isFalse();
  }

  private void createTypeAndCompact() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engineOf();
    appendRows(engine, 0, CRASH_RECOVERY_ROWS);
    engine.compactAll();
  }

  private TimeSeriesEngine engineOf() {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
  }

  private void appendRows(final TimeSeriesEngine engine, final int from, final int rows) throws IOException {
    final long[] timestamps = new long[rows];
    final Object[][] columns = new Object[2][rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = 1_700_000_000_000L + (from + i) * 1_000L;
      columns[0][i] = "host_" + ((from + i) % 7);
      columns[1][i] = (double) (from + i);
    }
    engine.appendBatch(timestamps, columns);
  }

  private long countSamples() {
    try (final ResultSet resultSet = database.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
      return resultSet.next().<Number>getProperty("cnt").longValue();
    }
  }

  // #9716
  // Regression test for issue #9716: a count of a TimeSeries type taken while a compaction publishes its sealed blocks
  // must not see the compacted rows twice - once in the new sealed blocks and once more in the mutable bucket the same
  // compaction has not cleared yet.
  // <p>
  // The race is made deterministic with {@link TimeSeriesShard#TEST_PHASE4C_AFTER_SWAP_HOOK}, which parks the compaction
  // inside Phase 4c exactly between the sealed-file swap and the mutable-bucket clear: the state the reporter's
  // {@code count(*)} landed in. The count is issued from another thread while the compaction is parked there; it either
  // answers at once (and, before the fix, answered both layers) or waits for the compaction to finish, and in both cases
  // its answer must be the number of rows inserted.

  private static final long T0   = 1_767_225_600_000L;
  private static final int  COUNT_ROWS = 3_000;

  // #9716
  @Test
  void sqlCountDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final long during = readDuringCompactionSwap(() -> sqlCount("SELECT count(*) AS c FROM T"));
    assertThat(during).isEqualTo(COUNT_ROWS);
    assertThat(sqlCount("SELECT count(*) AS c FROM T")).isEqualTo(COUNT_ROWS);
  }

  // #9716
  @Test
  void countTypeDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final long during = readDuringCompactionSwap(() -> database.countType("T", true));
    assertThat(during).isEqualTo(COUNT_ROWS);
  }

  // #9716
  @Test
  void countOfAMultiShardTypeDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(2);
    final long during = readDuringCompactionSwap(() -> sqlCount("SELECT count(*) AS c FROM T"));
    assertThat(during).isEqualTo(COUNT_ROWS);
  }

  // #9716
  /** The windowed form went through the aggregate push-down, which already held the lock: pinned so it stays that way. */
  @Test
  void windowedCountDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final long during = readDuringCompactionSwap(() -> sqlCount("SELECT count(*) AS c FROM T WHERE ts >= " + T0));
    assertThat(during).isEqualTo(COUNT_ROWS);
  }

  // #9716
  @Test
  void schemaTypesRecordsDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final Result during = readDuringCompactionSwap(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = 'T'")) {
        return rs.next();
      }
    });
    assertThat(((Number) during.getProperty("records")).longValue()).isEqualTo(COUNT_ROWS);
    final Result shard = during.<List<Result>>getProperty("shards").getFirst();
    assertThat(((Number) shard.getProperty("totalSamples")).longValue()).isEqualTo(COUNT_ROWS);
    assertThat(((Number) shard.getProperty("sealedSamples")).longValue()
        + ((Number) shard.getProperty("mutableSamples")).longValue()).isEqualTo(COUNT_ROWS);
  }

  private void createAndFill(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + shards);
    database.transaction(() -> {
      for (int i = 0; i < COUNT_ROWS; i++)
        database.command("sql", "INSERT INTO T SET ts = ?, host = ?, v = ?", T0 + i * 1000L, "h" + (i % 4), (double) i);
    });
    assertThat(sqlCount("SELECT count(*) AS c FROM T")).isEqualTo(COUNT_ROWS);
  }

  private long sqlCount(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * Compacts the type and runs {@code read} on another thread while the first shard's compaction is parked between the
   * sealed swap and the mutable clear. The hook holds the compaction there until the read has either answered (the
   * unfixed behavior: it read both layers without waiting) or parked on a shard's compaction lock (the fixed behavior:
   * it waits for this compaction and answers once it has finished). Both are observed, not timed, so a slow runner
   * cannot make the read miss the window. The 30 s bound is a hang guard only.
   */
  private <T> T readDuringCompactionSwap(final Callable<T> read) throws Exception {
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    final ExecutorService reader = Executors.newSingleThreadExecutor();
    final AtomicReference<Future<T>> pending = new AtomicReference<>();
    try {
      TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = () -> {
        if (pending.get() != null)
          return;
        final Future<T> future = reader.submit(read);
        pending.set(future);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!future.isDone() && !readerParkedOnACompactionLock(engine) && System.nanoTime() < deadline)
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      };

      engine.compactAll();
      TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = null;

      assertThat(pending.get()).as("the compaction reached the hook").isNotNull();
      return pending.get().get(30, TimeUnit.SECONDS);
    } finally {
      reader.shutdownNow();
    }
  }

  /**
   * Whether any thread waits on a shard's compaction lock. While the hook runs, the compaction holding the write lock
   * is the only other user of these locks in this test, so a queued thread is the reader; a test that adds other
   * readers or writers of the type would need to tell them apart.
   */
  private static boolean readerParkedOnACompactionLock(final TimeSeriesEngine engine) {
    for (int s = 0; s < engine.getShardCount(); s++)
      if (((ReentrantReadWriteLock) engine.getShard(s).getCompactionLock()).hasQueuedThreads())
        return true;
    return false;
  }
}
