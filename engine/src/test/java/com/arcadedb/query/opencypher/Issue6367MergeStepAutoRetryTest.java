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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6367 ("Small self-loop CREATE/MERGE query returns DeadlockDetected").
 * <p>
 * {@code CreateStep} wraps its writes in {@code database.transaction(..., true)}, which both joins
 * an already-active transaction (an explicit {@code BEGIN}, or the HTTP auto-commit wrapper in
 * {@code DatabaseAbstractHandler}) <em>and</em>, when it owns the transaction itself, retries
 * automatically on {@code NeedRetryException}/{@code ConcurrentModificationException} up to
 * {@link GlobalConfiguration#TX_RETRIES}. {@code MergeStep} (and {@code SetStep}, {@code DeleteStep},
 * {@code RemoveStep}, {@code ForeachStep}) instead call {@code begin()}/{@code commit()} directly, with
 * no retry at all.
 * <p>
 * Over HTTP this gap is invisible: {@code DatabaseAbstractHandler.executeInTransaction()} wraps the
 * *entire* autocommit command in one outer retry-capable transaction, so every write clause simply
 * joins it and a conflict retries the whole request. Bolt's {@code BoltNetworkExecutor.handleRun()}
 * calls {@code database.command(...)} directly with no such wrapper, so a write clause that owns its
 * own mini-transaction (the normal case for an autocommit Bolt {@code RUN}) has nothing to retry an
 * MVCC conflict with: the raw {@code NeedRetryException} propagates straight out and is reported to
 * the Bolt client as {@code Neo.TransientError.Transaction.DeadlockDetected} instead of being retried
 * transparently, the way {@code CreateStep} (and HTTP) already do.
 * <p>
 * This test drives concurrent, <b>unwrapped</b> {@code database.command("opencypher", ...)} calls
 * containing a standalone find-or-create {@code MERGE} against the same key - exactly the shape
 * {@code BoltNetworkExecutor.handleRun} uses for an autocommit write, and the most common real-world
 * use of {@code MERGE}. Before the fix this reliably surfaces
 * {@code NeedRetryException}/{@code ConcurrentModificationException} to the caller under contention;
 * after the fix every call succeeds because {@code MergeStep} retries on its own, exactly like
 * {@code CreateStep} already does - and MERGE's own match-or-create semantics (unaffected by this
 * change) still guarantee exactly one vertex is ever created.
 */
class Issue6367MergeStepAutoRetryTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-6367-merge-retry-test");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    // A handful of extra attempts keeps this reliable under CI contention without masking a genuine
    // regression: pre-fix, MergeStep does not retry at all, so raising the attempt count here changes
    // nothing for the unfixed code (every attempt is really attempt #1) but avoids a false failure
    // from ordinary retry exhaustion post-fix on a slow runner.
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 20);
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void mergeFindOrCreateRetriesOnMvccConflictWithoutAnOuterTransactionWrapper() throws InterruptedException {
    // A UNIQUE index on the matched property is what gives MERGE's find-or-create an atomicity guarantee
    // under concurrency in the first place (mirrors MergeInsertSlowdownTest's schema): without it, two
    // threads can both legitimately find no match and both create, since nothing conflicts. With it, a
    // losing thread's create collides on the index and mergeSingleNodeAll() re-runs the match, which is
    // the scenario this test drives contention through.
    database.command("sql", "CREATE VERTEX TYPE Cnt6367 IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY Cnt6367.id IF NOT EXISTS INTEGER");
    database.command("sql", "CREATE INDEX Cnt6367_PK IF NOT EXISTS ON Cnt6367 (id) UNIQUE");

    final int threads = 8;
    final int iterations = 25;

    final AtomicLong successCount = new AtomicLong();
    final AtomicLong conflictErrors = new AtomicLong();
    final AtomicLong otherErrors = new AtomicLong();

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final Thread worker = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (int i = 0; i < iterations; i++) {
          try {
            // Mirrors BoltNetworkExecutor.handleRun: a bare, unwrapped autocommit command - no outer
            // database.transaction() call here, exactly like Bolt's RUN handling for a write query.
            // Every thread races to find-or-create the SAME node: only one iteration across every
            // thread actually creates it, the rest match it.
            try (final ResultSet rs = database.command("opencypher",
                "MERGE (a:Cnt6367 {id:1}) RETURN a")) {
              assertThat(rs.hasNext()).isTrue();
              rs.next();
            }
            successCount.incrementAndGet();
          } catch (final Exception e) {
            if (CauseChain.find(e, NeedRetryException.class) != null)
              // Pre-fix: MergeStep has no retry of its own, so ordinary contention on the shared node
              // surfaces here directly (wrapped in CommandExecutionException by
              // OpenCypherQueryEngine.command()) - the same shape of failure Bolt reports as
              // DeadlockDetected. Bolt's own BoltNetworkExecutor.isRetryableConflict() classifies
              // conflicts the same way, via CauseChain, so this mirrors what actually reaches the wire.
              conflictErrors.incrementAndGet();
            else
              otherErrors.incrementAndGet();
          }
        }
        done.countDown();
      }, "issue-6367-merge-worker-" + t);
      worker.start();
    }

    start.countDown();
    done.await();

    assertThat(otherErrors.get()).as("unexpected non-conflict errors").isEqualTo(0);
    assertThat(conflictErrors.get())
        .as("MergeStep must retry an MVCC conflict on its own mini-transaction, exactly like CreateStep, "
            + "instead of surfacing it to an unwrapped autocommit caller (issue #6367)")
        .isEqualTo(0);
    assertThat(successCount.get()).isEqualTo((long) threads * iterations);

    final long vertexCount;
    try (final ResultSet rs = database.query("cypher", "MATCH (a:Cnt6367) RETURN count(a) AS c")) {
      vertexCount = ((Number) rs.next().getProperty("c")).longValue();
    }
    assertThat(vertexCount)
        .as("MERGE's match-or-create semantics must still hold under contention: exactly one vertex, never a duplicate")
        .isEqualTo(1);
  }
}
