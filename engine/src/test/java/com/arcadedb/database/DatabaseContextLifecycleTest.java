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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the DatabaseContext lifecycle findings of the 2026-07 engine audit:
 * <ul>
 *   <li>#4941: a thread that dies with an open transaction holding explicit file locks must have its
 *   transaction rolled back by the dead-thread sweep, releasing the locks; before, the sweep only dropped
 *   the map entry and the LockManager files stayed owned by the dead thread forever (every later commit
 *   timing out until restart).</li>
 *   <li>#4956: the sweep must not prune LIVE virtual threads (Thread.getAllStackTraces() returned platform
 *   threads only, so a live virtual thread's entry was removed while its transaction was running), must
 *   detect DEAD virtual threads, and the GraphAnalyticalView async workers must unregister their contexts.</li>
 *   <li>#4939: contexts maps are mutated concurrently by the owner thread and by close/drop on another
 *   thread; the concurrent hammering must never corrupt them (contract test: the race itself has no
 *   deterministic repro).</li>
 * </ul>
 */
class DatabaseContextLifecycleTest extends TestHelper {

  private static final long LOCK_TIMEOUT_MS = 2_000L;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, LOCK_TIMEOUT_MS);
    database.getSchema().getOrCreateDocumentType("Product");
  }

  @Test
  void deadPlatformThreadWithExplicitLockIsSweptAndLocksReleased() throws Exception {
    final Thread worker = new Thread(() -> {
      database.begin();
      database.acquireLock().type("Product").lock();
      // DIES WITHOUT COMMIT NOR ROLLBACK: THE EXPLICIT LOCKS ARE ABANDONED
    }, "ctx-lifecycle-dead-platform");
    worker.start();
    worker.join(10_000);
    assertThat(worker.isAlive()).isFalse();

    DatabaseContext.cleanupDeadThreads();

    assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isFalse();

    // THE SWEEP MUST HAVE ROLLED BACK THE ABANDONED TRANSACTION AND RELEASED ITS FILE LOCKS: A COMMIT
    // TOUCHING THE SAME TYPE MUST SUCCEED INSTEAD OF FAILING WITH LockTimeoutException
    database.begin();
    database.newDocument("Product").set("name", "after-sweep").save();
    database.commit();

    assertThat(database.countType("Product", true)).isEqualTo(1);
  }

  @Test
  void liveVirtualThreadContextSurvivesSweep() throws Exception {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<Throwable> workerError = new AtomicReference<>();

    final Thread worker = Thread.ofVirtual().name("ctx-lifecycle-live-virtual").start(() -> {
      try {
        database.begin();
        try {
          started.countDown();
          release.await(10, TimeUnit.SECONDS);
        } finally {
          database.rollback();
          DatabaseContext.INSTANCE.removeCurrentThreadContexts();
        }
      } catch (final Throwable e) {
        workerError.set(e);
      }
    });

    assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
    try {
      // THE WORKER IS ALIVE (PARKED INSIDE ITS TRANSACTION): THE SWEEP MUST NOT PRUNE ITS CONTEXT.
      // Thread.getAllStackTraces() BASED DETECTION PRUNED LIVE VIRTUAL THREADS BECAUSE THEY NEVER APPEAR IN IT
      DatabaseContext.cleanupDeadThreads();
      assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isTrue();
    } finally {
      release.countDown();
    }

    worker.join(10_000);
    assertThat(worker.isAlive()).isFalse();
    assertThat(workerError.get()).isNull();
    // THE WORKER UNREGISTERED ITSELF ON EXIT
    assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isFalse();
  }

  @Test
  void deadVirtualThreadWithExplicitLockIsSweptAndLocksReleased() throws Exception {
    final Thread worker = Thread.ofVirtual().name("ctx-lifecycle-dead-virtual").start(() -> {
      database.begin();
      database.acquireLock().type("Product").lock();
      // DIES WITHOUT COMMIT NOR ROLLBACK
    });
    worker.join(10_000);
    assertThat(worker.isAlive()).isFalse();

    DatabaseContext.cleanupDeadThreads();

    assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isFalse();

    database.begin();
    database.newDocument("Product").set("name", "after-virtual-sweep").save();
    database.commit();

    assertThat(database.countType("Product", true)).isEqualTo(1);
  }

  @Test
  void sweepPrunesEmptyEntryOfLiveThreadAfterForeignClose() throws Exception {
    final CountDownLatch registered = new CountDownLatch(1);
    final CountDownLatch pruned = new CountDownLatch(1);
    final CountDownLatch reinitDone = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<Throwable> workerError = new AtomicReference<>();

    final Thread worker = new Thread(() -> {
      try {
        database.begin();
        database.rollback();
        registered.countDown();
        // STAYS ALIVE AND IDLE, LIKE A LONG-LIVED POOL THREAD THAT OPENED AND CLOSED A DATABASE
        if (!pruned.await(10, TimeUnit.SECONDS))
          throw new IllegalStateException("prune signal not received");
        // AFTER THE PRUNE, THE OWNER'S NEXT init() MUST RE-REGISTER CLEANLY
        database.begin();
        database.rollback();
        reinitDone.countDown();
        release.await(10, TimeUnit.SECONDS);
      } catch (final Throwable e) {
        workerError.set(e);
      } finally {
        DatabaseContext.INSTANCE.removeCurrentThreadContexts();
      }
    }, "ctx-lifecycle-live-empty");
    worker.start();

    assertThat(registered.await(10, TimeUnit.SECONDS)).isTrue();
    try {
      assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isTrue();

      // A FOREIGN CLOSE (removeAllContexts ON THIS THREAD) EMPTIES THE WORKER'S PER-DATABASE MAP BUT
      // DELIBERATELY DOES NOT PRUNE ITS CONTEXTS ENTRY (#4939): THE EMPTY ENTRY LINGERS WHILE THE WORKER
      // IS ALIVE
      // NOTE (#5076 review): this empties the per-db map for EVERY thread that had the db open, including
      // THIS main test thread - the sweep may prune the main thread's now-empty entry too. Harmless: the
      // next context-API touch (teardown's init()) re-registers it; the assertions below key on the worker.
      DatabaseContext.INSTANCE.removeAllContexts(database.getDatabasePath());
      assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isTrue();

      // #5067: THE PERIODIC SWEEP MUST OPPORTUNISTICALLY DROP THE NOW-EMPTY ENTRY EVEN THOUGH ITS OWNER IS
      // STILL ALIVE, SO OPEN/CLOSE CHURN ON LARGE LONG-LIVED THREAD POOLS DOES NOT ACCUMULATE EMPTY ENTRIES
      DatabaseContext.cleanupDeadThreads();
      assertThat(DatabaseContext.isThreadRegistered(worker.threadId()))
          .as("the sweep must prune a live thread's empty context entry")
          .isFalse();

      // THE PRUNED OWNER RE-REGISTERS ON ITS NEXT init()
      pruned.countDown();
      assertThat(reinitDone.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isTrue();
    } finally {
      pruned.countDown();
      release.countDown();
    }

    worker.join(10_000);
    assertThat(worker.isAlive()).isFalse();
    assertThat(workerError.get()).isNull();
    assertThat(DatabaseContext.isThreadRegistered(worker.threadId())).isFalse();
  }

  @Test
  void gavAsyncBuildUnregistersWorkerContext() {
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("Knows");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
      a.newEdge("Knows", b);
    });

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("Knows")
        .buildAsync();
    try {
      assertThat(gav.awaitReady(10, TimeUnit.SECONDS)).isTrue();

      // THE ASYNC BUILD WORKER (A DIED-AFTER-TASK "gav-worker-*" VIRTUAL THREAD) MUST HAVE REMOVED ITS OWN
      // CONTEXT ENTRY BEFORE RELEASING THE READY LATCH; BEFORE THE FIX THE ENTRY LINGERED UNTIL THE NEXT
      // PERIODIC SWEEP. KEYED TO THE WORKER THREAD NAME PREFIX: A JVM-WIDE SNAPSHOT DIFF WOULD GO SPURIOUSLY
      // RED IF ANY UNRELATED THREAD REGISTERED A CONTEXT DURING THE BUILD
      assertThat(DatabaseContext.isThreadRegisteredWithNamePrefix("gav-worker-")).isFalse();
    } finally {
      gav.drop();
    }
  }

  @Test
  void concurrentSweepsRollBackAbandonedTransactionsExactlyOnce() throws Exception {
    // CONTRACT TEST: two threads can run cleanupDeadThreads() concurrently (both landing on the periodic
    // 1000th-init() boundary). Each dead entry must be CLAIMED atomically so its abandoned transactions are
    // rolled back exactly once; without the claim both sweepers could pass the liveness check on the same
    // entry and double-roll-back the same transaction (probabilistic race, no deterministic repro).
    final DatabaseInternal internal = (DatabaseInternal) database;

    final int workers = 16;
    final CountingTransaction[] transactions = new CountingTransaction[workers];
    final Thread[] threads = new Thread[workers];
    for (int i = 0; i < workers; i++) {
      final CountingTransaction tx = new CountingTransaction(internal);
      transactions[i] = tx;
      threads[i] = new Thread(() -> DatabaseContext.INSTANCE.init(internal, tx), "ctx-lifecycle-sweep-race-" + i);
      threads[i].start();
    }
    for (final Thread thread : threads) {
      thread.join(10_000);
      assertThat(thread.isAlive()).isFalse();
    }

    final CyclicBarrier barrier = new CyclicBarrier(2);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Runnable sweeper = () -> {
      try {
        barrier.await(10, TimeUnit.SECONDS);
        DatabaseContext.cleanupDeadThreads();
      } catch (final Throwable e) {
        error.set(e);
      }
    };
    final Thread sweeper1 = new Thread(sweeper, "ctx-lifecycle-sweeper-1");
    final Thread sweeper2 = new Thread(sweeper, "ctx-lifecycle-sweeper-2");
    sweeper1.start();
    sweeper2.start();
    sweeper1.join(10_000);
    sweeper2.join(10_000);
    assertThat(sweeper1.isAlive()).isFalse();
    assertThat(sweeper2.isAlive()).isFalse();
    assertThat(error.get()).isNull();

    for (int i = 0; i < workers; i++) {
      assertThat(transactions[i].rollbacks.get()).as("worker %d rollback count", i).isEqualTo(1);
      assertThat(DatabaseContext.isThreadRegistered(threads[i].threadId())).isFalse();
    }
  }

  @Test
  void sweepAndDatabaseCloseRollBackAbandonedTransactionsExactlyOnce() throws Exception {
    // CONTRACT TEST: LocalDatabase.closeInternal claims each per-thread context of the closing database via
    // removeAllContexts() and rolls it back under the DB write lock; the dead-thread sweep rolls back the
    // same contexts with NO db lock, so the commit-read-lock/close-write-lock isolation argument does not
    // cover this pair. Each abandoned transaction must be rolled back by exactly one of the two paths;
    // without the sweep's per-database value-keyed claim both could obtain the same tl and double-roll-back
    // it. The closeInternal side is simulated faithfully (removeAllContexts() plus rollback of the returned
    // contexts, as LocalDatabase.closeInternal does). The race window is a single iteration crossover per
    // sweep (both paths traverse CONTEXTS in the same order), so the race is retried over many rounds. NOTE:
    // even hammered, the pre-fix window (sub-microsecond, between the closer fetching the ThreadContexts and
    // its inner remove) never reproduced locally; this is an exactly-once CONTRACT test, not a red-first
    // repro - post-fix the invariant is guaranteed by the two-level atomic claim.
    // A DEDICATED THROWAWAY DATABASE ISOLATES THE SIMULATED CLOSE: removeAllContexts() ON THE SHARED LIVE
    // TEST DATABASE WOULD ALSO STRIP THE MAIN THREAD'S REAL REGISTRY ENTRY EVERY ROUND
    final Database throwaway = createThrowawayDatabase("_closerace");
    try {
      final DatabaseInternal internal = (DatabaseInternal) throwaway;
      final int rounds = 100;
      final int workers = 4;

      for (int round = 0; round < rounds; round++) {
        final CountingTransaction[] transactions = new CountingTransaction[workers];
        final Thread[] threads = new Thread[workers];
        for (int i = 0; i < workers; i++) {
          final CountingTransaction tx = new CountingTransaction(internal);
          transactions[i] = tx;
          threads[i] = new Thread(() -> DatabaseContext.INSTANCE.init(internal, tx), "ctx-lifecycle-close-race-" + i);
          threads[i].start();
        }
        for (final Thread thread : threads) {
          thread.join(10_000);
          assertThat(thread.isAlive()).isFalse();
        }

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread sweeper = new Thread(() -> {
          try {
            barrier.await(10, TimeUnit.SECONDS);
            DatabaseContext.cleanupDeadThreads();
          } catch (final Throwable e) {
            error.set(e);
          }
        }, "ctx-lifecycle-close-race-sweeper");
        final Thread closer = new Thread(() -> {
          try {
            barrier.await(10, TimeUnit.SECONDS);
            for (final DatabaseContext.DatabaseContextTL tl : DatabaseContext.INSTANCE.removeAllContexts(
                internal.getDatabasePath())) {
              for (int i = tl.transactions.size() - 1; i > -1; --i) {
                final TransactionContext tx = tl.transactions.get(i);
                if (tx.isActive())
                  tx.rollback();
              }
              tl.transactions.clear();
            }
          } catch (final Throwable e) {
            error.set(e);
          }
        }, "ctx-lifecycle-close-race-closer");
        sweeper.start();
        closer.start();
        sweeper.join(10_000);
        closer.join(10_000);
        assertThat(sweeper.isAlive()).isFalse();
        assertThat(closer.isAlive()).isFalse();
        assertThat(error.get()).isNull();

        for (int i = 0; i < workers; i++)
          assertThat(transactions[i].rollbacks.get()).as("round %d worker %d rollback count", round, i).isEqualTo(1);
      }
    } finally {
      throwaway.drop();
    }
  }

  @Test
  void concurrentInitAndRemoveAllContextsDoNotCorruptContexts() throws Exception {
    // CONTRACT TEST FOR #4939: the per-thread contexts map is mutated by the owner (init/removeContext) and
    // by removeAllContexts() from another thread. A deterministic corruption repro is impossible (pure data
    // race on the old plain HashMap); this hammers the two paths concurrently and asserts that no exception
    // surfaces and the owner thread's context always survives for the database that is never closed.
    // THE FOREIGN KEY IS A GENUINELY REGISTERED THROWAWAY DATABASE: THE OWNER RE-REGISTERS IT EVERY
    // ITERATION, SO removeAllContexts() EXERCISES A REAL SAME-KEY remove() ON THE OWNER'S MAP RACING THE
    // OWNER'S put() - A NEVER-REGISTERED NAME WOULD ONLY EXERCISE THE CONTEXTS ITERATION, NEVER THE REMOVAL
    final Database foreign = createThrowawayDatabase("_foreign");
    try {
      final DatabaseInternal internal = (DatabaseInternal) database;
      final DatabaseInternal foreignInternal = (DatabaseInternal) foreign;
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);

      final Thread owner = new Thread(() -> {
        try {
          for (int i = 0; i < 20_000; i++) {
            DatabaseContext.INSTANCE.init(internal);
            DatabaseContext.INSTANCE.init(foreignInternal);
            final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(
                internal.getDatabasePath());
            if (ctx == null)
              throw new IllegalStateException("Owner thread lost its own context at iteration " + i);
          }
        } catch (final Throwable e) {
          error.set(e);
        } finally {
          DatabaseContext.INSTANCE.removeCurrentThreadContexts();
          done.countDown();
        }
      }, "ctx-lifecycle-owner");
      owner.start();

      // HAMMER removeAllContexts FROM THIS THREAD (SIMULATES CLOSE/DROP OF THE FOREIGN DATABASE) WHILE THE
      // OWNER KEEPS MUTATING ITS OWN PER-THREAD MAP, RE-REGISTERING THE FOREIGN KEY EVERY ITERATION
      while (done.getCount() > 0) {
        DatabaseContext.INSTANCE.removeAllContexts(foreignInternal.getDatabasePath());
        if (!done.await(0, TimeUnit.MILLISECONDS))
          Thread.onSpinWait();
      }

      owner.join(30_000);
      assertThat(owner.isAlive()).isFalse();
      assertThat(error.get()).isNull();
    } finally {
      foreign.drop();
    }
  }

  @Test
  void sweepEmitsSingleSummaryWarningWhenItRollsBackWork() throws Exception {
    // OPERABILITY CONTRACT: a sweep that performs real rollback work must emit ONE attributable WARNING
    // (count + thread id + database), so the inline tail-latency blip on the triggering thread can be traced;
    // zero-work sweeps stay silent. The test raises the DatabaseContext logger to WARNING because the shared
    // test logging config caps com.arcadedb at SEVERE.
    final DatabaseInternal internal = (DatabaseInternal) database;
    final Logger sweepLogger = Logger.getLogger(DatabaseContext.class.getName());
    final List<LogRecord> warnings = Collections.synchronizedList(new ArrayList<>());
    final Handler capture = new Handler() {
      @Override
      public void publish(final LogRecord logRecord) {
        if (logRecord.getLevel() == Level.WARNING)
          warnings.add(logRecord);
      }

      @Override
      public void flush() {
        // NOTHING TO FLUSH
      }

      @Override
      public void close() {
        // NOTHING TO CLOSE
      }
    };
    final Level previousLevel = sweepLogger.getLevel();
    sweepLogger.setLevel(Level.WARNING);
    sweepLogger.addHandler(capture);
    try {
      // DRAIN ANY DEAD ENTRIES LEFT BY EARLIER TESTS IN THE SAME JVM, THEN PROVE A ZERO-WORK SWEEP IS SILENT
      DatabaseContext.cleanupDeadThreads();
      warnings.clear();
      DatabaseContext.cleanupDeadThreads();
      assertThat(warnings).isEmpty();

      final CountingTransaction tx = new CountingTransaction(internal);
      final Thread worker = new Thread(() -> DatabaseContext.INSTANCE.init(internal, tx), "ctx-lifecycle-sweep-log");
      worker.start();
      worker.join(10_000);
      assertThat(worker.isAlive()).isFalse();

      DatabaseContext.cleanupDeadThreads();

      assertThat(tx.rollbacks.get()).isEqualTo(1);
      assertThat(warnings).hasSize(1);
      final String message = warnings.getFirst().getMessage();
      assertThat(message).contains("1 abandoned transaction");
      assertThat(message).contains("threadId=" + worker.threadId());
      assertThat(message).contains(internal.getDatabasePath());
    } finally {
      sweepLogger.removeHandler(capture);
      sweepLogger.setLevel(previousLevel);
    }
  }

  /**
   * Creates a dedicated throwaway database next to the shared test one, so contract tests that simulate a
   * database close (removeAllContexts) do not reach into the live shared instance's registry entries.
   */
  private Database createThrowawayDatabase(final String suffix) {
    final DatabaseFactory factory = new DatabaseFactory(database.getDatabasePath() + suffix);
    if (factory.exists())
      factory.open().drop();
    return factory.create();
  }

  /**
   * Always-active transaction stub whose rollback only counts invocations: lets the exactly-once contract
   * tests detect a double rollback of the same abandoned transaction without touching real storage.
   */
  private static final class CountingTransaction extends TransactionContext {
    private final AtomicInteger rollbacks = new AtomicInteger();

    private CountingTransaction(final DatabaseInternal database) {
      super(database.getWrappedDatabaseInstance());
    }

    @Override
    public boolean isActive() {
      return true;
    }

    @Override
    public void rollback() {
      rollbacks.incrementAndGet();
    }
  }
}
