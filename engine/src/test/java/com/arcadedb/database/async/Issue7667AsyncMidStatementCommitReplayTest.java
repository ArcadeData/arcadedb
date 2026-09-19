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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7667: the #7615 retry-by-replay decided whether a finished async task was safe to replay
 * purely from {@code database.isTransactionActive()} when {@code execute()} returned. That conflates <i>a
 * transaction is active</i> with <i>it is the same transaction the earlier buffered commands wrote to</i>.
 * <p>
 * A statement that commits MID-EXECUTION and immediately begins another breaks the second one. {@code BatchStep}
 * ({@code UPDATE}/{@code DELETE}/{@code MOVE VERTEX ... BATCH n}) does exactly that, and {@code TRUNCATE
 * TYPE}/{@code BUCKET} and {@code REBUILD INDEX} have the same shape. On return a transaction IS active - the
 * trailing {@code begin()} - so every command buffered ahead of it stayed in {@code pendingBatchCommands} even
 * though the mid-statement {@code commit()} had just made their writes durable. A later boundary conflict then
 * replayed the whole list on top of them.
 * <p>
 * The fix makes "the shared batch transaction was committed out from under us" observable
 * ({@code TransactionContext.getCommitCount()}) and treats it exactly as the out-of-band commit sites
 * ({@code DatabaseAsyncIndexCompaction}, {@code DatabaseAsyncParkWorker}, {@code DatabaseAsyncTransaction}) treat
 * their own commits: the buffered state is dropped, unnotified, because it is already durable.
 * <p>
 * Both tests below pass against a build with the fix and fail against one without it - the first with twice the
 * submitted records in the database, the second with an {@code onError} for every write that actually landed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7667AsyncMidStatementCommitReplayTest extends TestHelper {

  private static final String TYPE     = "Issue7667Item";
  private static final String OLD_TYPE = "Issue7667Old";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
    database.getSchema().createDocumentType(OLD_TYPE).createProperty("k", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument(OLD_TYPE).set("k", "k" + i).save();
    });
  }

  /**
   * Five inserts buffered, then a {@code DELETE ... BATCH 1} whose mid-statement commit publishes them, then a
   * boundary commit that conflicts. The conflict must not replay the five inserts a second time on top of the
   * copies that are already on disk.
   */
  @Test
  void midStatementCommitIsNotReplayedOnALaterBoundaryConflict() throws Exception {
    final int buffered = 5;

    database.async().setParallelLevel(1); // deterministic: one worker, one batch, one boundary commit
    // 7 = the five inserts + the BATCH statement + one more insert, so the boundary lands AFTER the mid-statement
    // commit rather than on it.
    database.async().setCommitEvery(7);

    final AtomicInteger completions = new AtomicInteger();
    database.async().onError(e -> {
      // Expected on this path: the batch cannot be retried by replay once a mid-statement commit split it, so the
      // conflict is reported rather than silently resolved. What must NOT happen is a duplicate write.
    });

    // Only the FIRST boundary commit conflicts, so the retry's replay is allowed to succeed and commit - which is
    // precisely what publishes the duplicates when the replay list still holds commands a mid-statement commit
    // already made durable. A hook that threw on every attempt would roll every replay back and hide the bug.
    final AtomicInteger hookCalls = new AtomicInteger();
    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
      if (hookCalls.incrementAndGet() == 1)
        throw new ConcurrentModificationException("simulated conflict at the boundary commit after a BATCH statement");
    };

    try {
      for (int i = 0; i < buffered; i++)
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", countingCallback(completions), i);

      // BATCH 1 => BatchStep commits the worker's shared batch after every deleted row, publishing the five
      // inserts above, and begins a fresh transaction each time.
      database.async().command("sql", "DELETE FROM " + OLD_TYPE + " BATCH 1", countingCallback(completions));

      // Trips the commitEvery boundary, whose commit the hook turns into a conflict.
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", countingCallback(completions), buffered);

      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
    }

    database.transaction(() -> assertThat(database.countType(TYPE, true))
        .as("the five inserts the BATCH statement made durable must exist exactly once, not twice: "
            + "the conflicting boundary commit must not replay commands a mid-statement commit already published")
        .isEqualTo(buffered));
  }

  /**
   * The mirror of the same root cause: the statement commits the shared batch part-way through and only THEN
   * fails. The commands buffered ahead of it are durable, so they must not be told {@code onError} - the
   * {@code AsyncResultsetCallback} javadoc documents that signal as authoritative.
   */
  @Test
  void aFailureAfterAMidStatementCommitDoesNotDenyWritesThatLanded() throws Exception {
    final int buffered = 5;

    // A unique index makes the SECOND row of the BATCH statement fail, after the FIRST one's commit already
    // published everything buffered ahead of it.
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, OLD_TYPE, "k");

    database.async().setParallelLevel(1);
    database.async().setCommitEvery(1000); // no periodic boundary: the only commit is the statement's own

    final AtomicInteger                   completions = new AtomicInteger();
    final ConcurrentLinkedQueue<Exception> errors      = new ConcurrentLinkedQueue<>();

    for (int i = 0; i < buffered; i++) {
      final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          completions.incrementAndGet();
        }

        @Override
        public void onError(final Exception e) {
          errors.add(e);
        }
      };
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
    }

    // Row 1 gets k='dup' and commits (publishing the five inserts); row 2 gets the same value and the commit that
    // BatchStep fires for it raises a duplicated-key failure out of the middle of the statement.
    database.async().command("sql", "UPDATE " + OLD_TYPE + " SET k = 'dup' BATCH 1", new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
      }

      @Override
      public void onError(final Exception e) {
        // The statement's own failure, which legitimately belongs to it.
      }
    });

    database.async().waitCompletion();

    assertThat(errors)
        .as("the five inserts were published by the statement's own mid-execution commit before it failed, so "
            + "their submitters must not be told the batch could not be made durable")
        .isEmpty();
    assertThat(completions.get()).isEqualTo(buffered);

    database.transaction(() -> assertThat(database.countType(TYPE, true))
        .as("and the writes those callbacks were told about really are on disk").isEqualTo(buffered));
  }

  /**
   * The counterpart the detection must NOT match (code review on PR #7850): a task that ROLLS the shared batch
   * back mid-execution and begins another one has destroyed the buffered writes rather than published them, so
   * their submitters must still be told when the batch is abandoned. Keying the detection on the transaction's
   * commit count rather than on a begin counter is what keeps these two apart - a begin counter moves identically
   * for both, and would silence the report for writes that really were lost.
   */
  @Test
  void aMidStatementRollbackIsNotMistakenForAMidStatementCommit() throws Exception {
    final int buffered = 3;

    database.async().setParallelLevel(1);
    database.async().setCommitEvery(1000); // no periodic boundary: the abandon below is the only outcome

    final ConcurrentLinkedQueue<Exception> errors = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < buffered; i++)
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
        }

        @Override
        public void onError(final Exception e) {
          errors.add(e);
        }
      }, i);

    // A task that rolls the shared batch back and re-opens one, then abandons the batch - the exact shape the
    // commit-count detection has to keep telling apart from a BatchStep commit.
    final CountDownLatch done = new CountDownLatch(1);
    ((DatabaseAsyncExecutorImpl) database.async()).scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
        database.rollback();
        database.begin();
        async.notifyPendingBatchCommandsAndAbandon(new RuntimeException("the batch these writes were in was rolled back"));
        done.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }, true, 0);

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    database.async().waitCompletion();

    assertThat(errors)
        .as("a rollback destroyed the buffered writes, so unlike a mid-statement COMMIT their submitters must "
            + "still be told - the detection must not treat the two alike")
        .hasSize(buffered);

    database.transaction(() -> assertThat(database.countType(TYPE, true))
        .as("and nothing of the rolled-back batch survived").isZero());
  }

  private static AsyncResultsetCallback countingCallback(final AtomicInteger completions) {
    return new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
        completions.incrementAndGet();
      }

      @Override
      public void onError(final Exception e) {
      }
    };
  }
}
