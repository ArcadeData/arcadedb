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

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12805: the after-completion callbacks of a transaction (#10687) run on a rollback only by the thread that carries the
 * transaction, so that the sweep of a dead thread's transaction does not run them on a thread in the middle of work of its own. A
 * transaction of a protocol session moves between threads, one per request, and the thread that carried it was always the one that
 * registered the callbacks: a rollback by the session's own client on another worker dropped them.
 * {@link TransactionContext#transferToCurrentThread()} hands them to the thread that carries the transaction now.
 */
class Issue12805AfterCompletionCarrierTest extends TestHelper {
  private static final String KEY = "issue12805";

  @Test
  void aRollbackByTheThreadTheTransactionWasTransferredToRunsTheCallbacks() throws Exception {
    final List<String> ranOn = new CopyOnWriteArrayList<>();
    final TransactionContext tx = beginOnWorker();
    onWorker("issue12805-register", () -> bound(tx, () -> registerCallback(tx, ranOn)));

    onWorker("issue12805-rollback", () -> bound(tx, () -> {
      tx.transferToCurrentThread();
      database.rollback();
    }));

    assertThat(tx.isActive()).isFalse();
    assertThat(ranOn).as("the thread carrying the transaction runs what its end owes").containsExactly("issue12805-rollback");
  }

  /**
   * A request that failed because its thread was interrupted rolls back with the flag set. The callbacks commit work of their own,
   * and a file channel used by a thread with the flag set is closed under it for every thread: they run with the flag cleared,
   * and the thread gets it back.
   */
  @Test
  void aRollbackByAnInterruptedCarrierRunsTheCallbacksWithTheFlagClearedAndHandsItBack() throws Exception {
    final List<Boolean> interruptedInCallback = new CopyOnWriteArrayList<>();
    final AtomicBoolean interruptedAfterRollback = new AtomicBoolean();
    final TransactionContext tx = beginOnWorker();
    onWorker("issue12805-register", () -> bound(tx,
        () -> assertThat(tx.addAfterCompletionCallbackIfAbsent(KEY,
            () -> interruptedInCallback.add(Thread.currentThread().isInterrupted()))).isTrue()));

    onWorker("issue12805-rollback", () -> bound(tx, () -> {
      tx.transferToCurrentThread();
      Thread.currentThread().interrupt();
      try {
        tx.rollback();
      } finally {
        // read and cleared: the thread ends here, and the cleanup of bound() must not run with it set
        interruptedAfterRollback.set(Thread.interrupted());
      }
    }));

    assertThat(tx.isActive()).isFalse();
    assertThat(interruptedInCallback).as("the callback runs with the flag cleared").containsExactly(false);
    assertThat(interruptedAfterRollback).as("the flag is handed back to the thread").isTrue();
  }

  /** What makes the difference: the rollback of a thread the transaction was not handed to still drops them. */
  @Test
  void aRollbackByAThreadTheTransactionWasNotTransferredToDropsTheCallbacks() throws Exception {
    final List<String> ranOn = new CopyOnWriteArrayList<>();
    final TransactionContext tx = beginOnWorker();
    onWorker("issue12805-register", () -> bound(tx, () -> registerCallback(tx, ranOn)));

    onWorker("issue12805-rollback", () -> bound(tx, () -> database.rollback()));

    assertThat(tx.isActive()).isFalse();
    assertThat(ranOn).isEmpty();
  }

  /** The sweep of a thread that died carrying the transaction, the callbacks handed to it, rolls it back and still drops them. */
  @Test
  void theSweepOfADeadCarrierStillDropsTheCallbacks() throws Exception {
    final List<String> ranOn = new CopyOnWriteArrayList<>();
    final TransactionContext tx = beginOnWorker();
    onWorker("issue12805-register", () -> bound(tx, () -> registerCallback(tx, ranOn)));

    // dies carrying the transaction, its context still registered
    final Thread carrier = new Thread(() -> {
      DatabaseContext.INSTANCE.init((DatabaseInternal) database, tx);
      tx.transferToCurrentThread();
    }, "issue12805-dead-carrier");
    carrier.start();
    carrier.join(10_000);
    assertThat(carrier.isAlive()).isFalse();

    DatabaseContext.cleanupDeadThreads();

    assertThat(DatabaseContext.isThreadRegistered(carrier.threadId())).isFalse();
    assertThat(tx.isActive()).as("the sweep rolled the abandoned transaction back").isFalse();
    assertThat(ranOn).as("on the sweeping thread, which has no business running them").isEmpty();
  }

  private static void registerCallback(final TransactionContext tx, final List<String> ranOn) {
    assertThat(tx.addAfterCompletionCallbackIfAbsent(KEY, () -> ranOn.add(Thread.currentThread().getName()))).isTrue();
  }

  /** What an HTTP {@code /begin} does: a transaction begun on a worker and left without a thread until the next request. */
  private TransactionContext beginOnWorker() throws Exception {
    final AtomicReference<TransactionContext> tx = new AtomicReference<>();
    onWorker("issue12805-begin", () -> {
      final DatabaseInternal db = (DatabaseInternal) database;
      DatabaseContext.INSTANCE.init(db);
      try {
        db.begin();
        tx.set(db.getTransaction());
      } finally {
        DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
      }
    });
    return tx.get();
  }

  /** One request: the transaction bound to the current thread for the time of {@code work}. */
  private void bound(final TransactionContext tx, final Runnable work) {
    final DatabaseInternal db = (DatabaseInternal) database;
    DatabaseContext.INSTANCE.init(db, tx);
    try {
      work.run();
    } finally {
      DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
    }
  }

  private static void onWorker(final String name, final Runnable work) throws InterruptedException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        work.run();
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, name);
    worker.start();
    worker.join();
    assertThat(failure.get()).isNull();
  }
}
