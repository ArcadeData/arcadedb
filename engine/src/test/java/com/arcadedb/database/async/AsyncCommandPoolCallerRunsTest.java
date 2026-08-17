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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the caller-runs fallback of {@link AsyncCommandPool} (issue #6303, item 3).
 * <p>
 * This is the path most of the pool's safety argument is about, and the one that never runs in a healthy process:
 * on saturation the dispatched statement executes on the thread that SUBMITTED it, which for the transport this
 * dispatch exists for is an HTTP worker in the middle of its own request. Two properties have to hold there and
 * nowhere else does anything check them - the submitter must count as a pool thread while it is running the command
 * (or a dispatched {@code CREATE INDEX} run inline would reach the barrier and wait for the in-flight-command set it
 * is itself a member of), and it must stop counting as one afterwards (or the rest of that request would inherit the
 * exemption).
 * <p>
 * Driven through the package-private policy rather than by saturating the real pool: filling a 1024-deep queue
 * behind a full set of blocking statements is a thousand-statement test of the queue, not of the fallback.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AsyncCommandPoolCallerRunsTest extends TestHelper {
  private static final int QUEUE_CAPACITY = 8;
  private static final int POOL_THREADS   = 4;

  @Test
  @Timeout(60)
  void theRejectedCommandRunsOnTheSubmitterAndIsCountedAsAFallback() {
    final AsyncCommandPool pool = AsyncCommandPool.getInstance();
    final long fallbacksBefore = pool.getPoolStats().callerRunFallbacks();

    final AtomicBoolean ran = new AtomicBoolean();
    final Thread submitter = Thread.currentThread();
    pool.runOnCaller(() -> {
      ran.set(true);
      assertThat(Thread.currentThread()).as("caller-runs means exactly that: the submitter executes it")
          .isSameAs(submitter);
    }, QUEUE_CAPACITY, POOL_THREADS);

    assertThat(ran).as("a rejected command must never be dropped: it has already been counted as in flight").isTrue();
    assertThat(pool.getPoolStats().callerRunFallbacks()).as(
        "the fallback is what an operator sees instead of inferring saturation from latency")
        .isEqualTo(fallbacksBefore + 1);
  }

  /**
   * The exemption that keeps a dispatched index build from waiting for itself has to be true on this path too, and
   * false again the moment the command is done.
   */
  @Test
  @Timeout(60)
  void theSubmitterCountsAsAPoolThreadOnlyWhileTheCommandRuns() {
    assertThat(AsyncCommandPool.isPoolThread()).as("precondition: an ordinary thread is not one of the pool's")
        .isFalse();

    final AtomicBoolean seenInside = new AtomicBoolean();
    AsyncCommandPool.getInstance()
        .runOnCaller(() -> seenInside.set(AsyncCommandPool.isPoolThread()), QUEUE_CAPACITY, POOL_THREADS);

    assertThat(seenInside).as(
        "a command run inline must be recognizable as a dispatched command, or the barrier it reaches waits for it")
        .isTrue();
    assertThat(AsyncCommandPool.isPoolThread()).as(
        "and the submitter must stop being one, or the rest of its request inherits the exemption").isFalse();
  }

  /** Restored, not cleared: a command that dispatches another must not lose the flag when the inner one returns. */
  @Test
  @Timeout(60)
  void aNestedFallbackRestoresTheFlagRatherThanClearingIt() {
    final AsyncCommandPool pool = AsyncCommandPool.getInstance();
    final AtomicBoolean stillSetAfterNested = new AtomicBoolean();

    pool.runOnCaller(() -> {
      pool.runOnCaller(() -> {
      }, QUEUE_CAPACITY, POOL_THREADS);
      stillSetAfterNested.set(AsyncCommandPool.isPoolThread());
    }, QUEUE_CAPACITY, POOL_THREADS);

    assertThat(stillSetAfterNested).isTrue();
    assertThat(AsyncCommandPool.isPoolThread()).as("and the outermost one still hands the thread back clean")
        .isFalse();
  }

  /** A statement that throws must not leave the submitter marked as a pool thread for the rest of its life. */
  @Test
  @Timeout(60)
  void aFailingCommandStillGivesTheSubmitterBack() {
    assertThatThrownBy(() -> AsyncCommandPool.getInstance().runOnCaller(() -> {
      throw new IllegalStateException("boom");
    }, QUEUE_CAPACITY, POOL_THREADS)).isInstanceOf(IllegalStateException.class);

    assertThat(AsyncCommandPool.isPoolThread()).isFalse();
  }

  /**
   * <b>The transaction-ownership property the caller-runs path rests on.</b> On the pool a failing command must
   * PROPAGATE rather than roll back, because the active transaction may not be its own: under the fallback the
   * statement runs on the submitting thread, which for {@code POST /command} is an HTTP worker already inside its
   * request's transaction. Rolling that back would end the request's transaction from underneath the handler that
   * owns it - after a 202 has very likely already gone out.
   * <p>
   * Driven at the task itself, with a live ambient transaction, because that is where the decision is: {@code async}
   * is what tells the task which of its two homes it is in, and {@code null} is the pool. The runner is then the one
   * place that knows whether the transaction is its own, and the only one that may discard it.
   */
  @Test
  @Timeout(120)
  void aFailingCommandOnThePoolPathLeavesTheCallersTransactionAlone() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1));

    final AtomicBoolean reported = new AtomicBoolean();
    final DatabaseAsyncCommand task = new DatabaseAsyncCommand(database.getConfiguration(), false, "sql",
        "CREATE PROPERTY NoSuchTypeHere.id INTEGER", (Object[]) null, new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
      }

      @Override
      public void onError(final Exception exception) {
        reported.set(true);
      }
    });

    database.begin();
    try {
      // The evidence: a rollback of the caller's transaction would take this with it.
      database.newDocument("V").save();

      assertThatThrownBy(() -> task.execute(null, (DatabaseInternal) database)).as(
          "on the pool the failure belongs to the runner, which is the only one that knows whose transaction is open")
          .isInstanceOf(Exception.class);

      assertThat(reported).as("...and the task does not report it either: reporting twice is how one failure becomes "
          + "two, and the runner reports through the same callback").isFalse();
      assertThat(database.isTransactionActive()).as(
          "a statement that failed inline must not end the transaction it was run inside").isTrue();
      assertThat(database.countType("V", false)).as("...nor discard what that transaction had already written")
          .isEqualTo(1);
    } finally {
      if (database.isTransactionActive())
        database.commit();
    }

    assertThat(database.countType("V", false)).isEqualTo(1);
  }

  /** And the submitter is still told: the runner delivers through the same callback the task would have used. */
  @Test
  @Timeout(120)
  void aFailingDispatchedDDLStillReachesTheSubmittersCallback() throws Exception {
    final CountDownLatch reported = new CountDownLatch(1);
    database.async().command("sql", "CREATE PROPERTY NoSuchTypeHere.id INTEGER", new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
      }

      @Override
      public void onError(final Exception exception) {
        reported.countDown();
      }
    });

    assertThat(reported.await(60, TimeUnit.SECONDS)).as(
        "moving the rollback to the runner must not move the report with it").isTrue();
    database.async().waitCompletion();
  }
}
