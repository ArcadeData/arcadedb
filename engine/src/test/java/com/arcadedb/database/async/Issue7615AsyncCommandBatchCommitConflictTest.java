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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7615: {@code db.async().command(...)} at a {@code commitEvery} > 1 batch loses records
 * silently when the periodic boundary commit throws {@link ConcurrentModificationException}.
 * <p>
 * Every {@link DatabaseAsyncCommand} in the still-open batch already told its own {@link AsyncResultsetCallback}
 * {@code onComplete} - a non-durable signal (issue #6470) - before the batch's periodic commit runs. Without the
 * fix, a {@link ConcurrentModificationException} from that commit rolled the whole batch back with no retry and no
 * per-command notification: only the executor-wide {@link DatabaseAsyncExecutor#onError(ErrorCallback)} fired, once,
 * for whichever command happened to trigger the boundary - the writes vanished with nothing telling their own
 * submitters they never landed.
 * <p>
 * {@link DatabaseAsyncExecutorImpl#TEST_BEFORE_BATCH_COMMIT_HOOK} reproduces the conflict deterministically instead
 * of relying on genuine cross-worker page contention (which is what the original report needed a real multi-worker
 * bulk load to observe).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7615AsyncCommandBatchCommitConflictTest extends TestHelper {

  private static final String TYPE = "Issue7615Item";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
  }

  /**
   * A ConcurrentModificationException on the FIRST attempt of a periodic boundary commit must be retried
   * transparently: every command in the batch is replayed under a fresh transaction, none of their onComplete
   * callbacks fire a second time, and every single submitted record ends up durably stored.
   */
  @Test
  void transientConflictAtBoundaryCommitIsRetriedAndNoRecordIsLost() throws Exception {
    final int total = 23;

    database.async().setParallelLevel(1); // deterministic: exactly one worker's boundary commit hits the hook
    database.async().setCommitEvery(5);

    final AtomicInteger completions = new AtomicInteger();
    final AtomicInteger errors      = new AtomicInteger();
    database.async().onError(e -> errors.incrementAndGet());

    final AtomicInteger hookCalls = new AtomicInteger();
    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
      if (hookCalls.incrementAndGet() == 1)
        throw new ConcurrentModificationException("simulated conflict at the first boundary commit");
    };

    try {
      for (int i = 0; i < total; i++) {
        final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
            completions.incrementAndGet();
          }

          @Override
          public void onError(final Exception e) {
            errors.incrementAndGet();
          }
        };
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
      }

      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
    }

    assertThat(errors.get()).as("no per-command or executor-wide error: the conflict was retried transparently").isZero();
    // Every command's onComplete fires exactly once - the replay after the simulated conflict must not
    // re-invoke it for commands that were already told "applied" on their first (rolled-back) attempt.
    assertThat(completions.get()).isEqualTo(total);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(total));
  }

  /**
   * When the boundary commit fails on every retry attempt, every command buffered in that batch must get its own
   * onError - not just the single executor-wide callback - and none of the batch's writes may be left in the
   * database.
   */
  @Test
  void exhaustedRetriesNotifyEveryBufferedCommandOwnErrorCallback() throws Exception {
    database.async().setParallelLevel(1); // deterministic: exactly one worker's boundary commit hits the hook
    database.async().setCommitEvery(5);

    final AtomicInteger executorWideErrors = new AtomicInteger();
    database.async().onError(e -> executorWideErrors.incrementAndGet());

    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK =
        callNumber -> {
          throw new ConcurrentModificationException("simulated permanent conflict");
        };

    final List<Exception> perCommandErrors = new CopyOnWriteArrayList<>();
    final AtomicInteger    perCommandOks    = new AtomicInteger();

    try {
      for (int i = 0; i < 5; i++) {
        final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
            perCommandOks.incrementAndGet();
          }

          @Override
          public void onError(final Exception e) {
            perCommandErrors.add(e);
          }
        };
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
      }

      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
    }

    assertThat(executorWideErrors.get()).as("the executor-wide callback still fires, unchanged").isGreaterThanOrEqualTo(1);

    // Every buffered command already got onComplete on its first, now-discarded attempt (the non-durable
    // signal from #6470) - that is unchanged by this fix and still fires exactly once per command.
    assertThat(perCommandOks.get()).as("onComplete still fires once per command on its first attempt, unchanged").isEqualTo(5);

    // The actual fix: every one of the 5 buffered commands is ALSO told its own onError once the retries
    // are exhausted, instead of the failure being visible only through the executor-wide callback above -
    // in addition to the onComplete above, which is the point (see commitBatch()'s own javadoc).
    assertThat(perCommandErrors).hasSize(5);
    for (final Exception e : perCommandErrors)
      assertThat(e).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
  }

  /**
   * The original report's own shape, without fault injection: several workers hammering a unique index produce
   * genuine periodic-boundary {@link ConcurrentModificationException}s from real cross-worker page contention. Every
   * submitted command must now end up either durably stored or reported through its own {@code onError} - never
   * both silent and missing, which is what #7615 observed (thousands of records neither stored nor erred on).
   */
  @Test
  void concurrentWorkersUnderRealContentionLoseNothingSilently() throws Exception {
    database.getSchema().getType(TYPE).createProperty("seq", Integer.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "seq");

    final int total = 4_000;

    database.async().setParallelLevel(4);
    database.async().setCommitEvery(97); // Deliberately not a divisor of `total`, to also exercise the tail flush.

    final AtomicInteger completions = new AtomicInteger();
    final AtomicInteger errors      = new AtomicInteger();

    for (int i = 0; i < total; i++) {
      final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          completions.incrementAndGet();
        }

        @Override
        public void onError(final Exception e) {
          errors.incrementAndGet();
        }
      };
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
    }

    database.async().waitCompletion();

    final AtomicLong stored = new AtomicLong();
    database.transaction(() -> stored.set(database.countType(TYPE, true)));

    // The core guarantee #7615 asks for: every submission is accounted for, either as a durable record or as an
    // onError on its own callback - never silently neither.
    assertThat(stored.get() + errors.get())
        .as("stored (%d) + errored (%d) must account for every one of the %d submitted commands", stored.get(), errors.get(), total)
        .isEqualTo(total);
    assertThat(completions.get()).as("onComplete must not fire more than once per submitted command").isLessThanOrEqualTo(total);
  }

  /**
   * commitBatch() can only replay what it buffers in {@code pendingBatchCommands} - plain {@code command()} calls.
   * A batch that ALSO ran a {@link DatabaseAsyncExecutor#createRecord} (or updateRecord/deleteRecord) cannot be
   * safely replayed: doing so anyway would silently omit that write while still reporting the retry as a clean
   * success. This must fall back to a single attempt instead - no retry, batch abandoned - with both the buffered
   * command AND the unreplayable task told via their own error callback ({@code DatabaseAsyncCommand#notifyError}
   * and {@code DatabaseAsyncTask#notifyBatchAbandoned} respectively), rather than fabricating a commit that never
   * actually included the unreplayable write.
   */
  @Test
  void mixedBatchWithAnUnreplayableWriteIsNeverSilentlyFabricatedAsASuccess() throws Exception {
    database.async().setParallelLevel(1); // deterministic: no idle worker's own empty-transaction commit competes for the hook
    database.async().setCommitEvery(2);

    final AtomicInteger hookCalls = new AtomicInteger();
    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
      hookCalls.incrementAndGet();
      throw new ConcurrentModificationException("simulated conflict on a mixed, unreplayable batch");
    };

    final AtomicInteger commandErrors = new AtomicInteger();
    final AtomicInteger createErrors  = new AtomicInteger();

    try {
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
        }

        @Override
        public void onError(final Exception e) {
          commandErrors.incrementAndGet();
        }
      }, 0);

      // The 2nd task in the batch (commitEvery=2): a task type commitBatch() cannot replay.
      database.async().createRecord(database.newDocument(TYPE), record -> {
      }, e -> createErrors.incrementAndGet());

      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
    }

    // No retry attempted for an unreplayable batch: exactly one commit attempt, not up to TX_RETRIES+1.
    assertThat(hookCalls.get()).as("a mixed batch must not be retried - there is nothing safe to replay it with").isEqualTo(1);
    assertThat(commandErrors.get()).as("the buffered command is still told, unlike before this fix").isEqualTo(1);
    // The unreplayable createRecord task is told too, via DatabaseAsyncTask#notifyBatchAbandoned - not just
    // the executor-wide onError a caller might not have wired up.
    assertThat(createErrors.get()).as("the unreplayable task is told its write never landed too").isEqualTo(1);

    // Nothing from this abandoned batch may be durably stored - the guard exists precisely so a partial replay can
    // never masquerade as a complete one.
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
  }

  /**
   * Code review on this PR flagged that {@link DatabaseAsyncIndexCompaction} and {@link DatabaseAsyncParkWorker}
   * commit the same shared, worker-owned batch transaction directly, bypassing {@code commitBatch()} entirely - so a
   * conflict there used to leave {@code pendingBatchCommands} either stale (index compaction: no try/catch at all, so
   * the whole {@code executeTask()} postprocessing block that would have cleared it was skipped) or silently cleared
   * with nobody notified (park worker: caught, but only the executor-wide callback was told). Both now route through
   * the same {@code clearPendingBatchCommands()}/{@code notifyPendingBatchCommandsAndAbandon()} choke points
   * {@code commitBatch()} uses.
   * <p>
   * {@code index compaction} runs with {@code requiresActiveTx() == false} and is dispatched via
   * {@code DatabaseAsyncExecutorImpl.compact()} exactly as the real {@code onAfterCommit} hook does when an LSM index
   * crosses its compaction threshold mid-batch - the scenario the PR's own {@code concurrentWorkersUnderRealContentionLoseNothingSilently}
   * test exercises for real, reproduced here deterministically via the same fault-injection hook.
   */
  @Test
  void indexCompactionAbandoningTheBatchNotifiesEveryBufferedCommand() throws Exception {
    database.async().setParallelLevel(1); // deterministic: no other worker's own commit competes for the hook
    database.async().setCommitEvery(1_000); // large enough that only the compaction's own commit triggers the hook

    final Index index = database.getSchema().getType(TYPE).createProperty("seq", Integer.class)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    // The per-bucket index, the same one DatabaseAsyncIndexCompaction actually compacts (see
    // DatabaseAsyncExecutorCompactionShutdownRaceTest for the identical extraction).
    final IndexInternal indexInternal = ((TypeIndex) index).getIndexesOnBuckets()[0];

    final AtomicInteger executorWideErrors = new AtomicInteger();
    database.async().onError(e -> executorWideErrors.incrementAndGet());

    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK =
        callNumber -> {
          throw new ConcurrentModificationException("simulated conflict on the compaction task's own commit");
        };

    final List<Exception> perCommandErrors = new CopyOnWriteArrayList<>();

    try {
      for (int i = 0; i < 3; i++) {
        final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
          }

          @Override
          public void onError(final Exception e) {
            perCommandErrors.add(e);
          }
        };
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
      }

      // Dispatched to the same worker (parallel level 1), landing mid-batch behind the 3 commands above -
      // exactly like the real onAfterCommit hook would once the index crosses its compaction threshold.
      ((DatabaseAsyncExecutorImpl) database.async()).compact(indexInternal);

      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
    }

    assertThat(executorWideErrors.get()).as("the executor-wide callback still fires, unchanged").isGreaterThanOrEqualTo(1);

    // The actual fix: all 3 commands buffered ahead of the compaction task are told their own onError, instead of
    // the failure being visible only through the executor-wide callback above (or, pre-fix, not at all for the
    // no-try/catch compaction path).
    assertThat(perCommandErrors).hasSize(3);
    for (final Exception e : perCommandErrors)
      assertThat(e).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
  }

  /**
   * Same gap as {@link #indexCompactionAbandoningTheBatchNotifiesEveryBufferedCommand}, for
   * {@link DatabaseAsyncParkWorker} - the task {@code quiesceWorkers()} uses to commit and pause each worker (issue
   * #6303 item 2). Unlike the compaction task it already caught its own commit failure, but only told the
   * executor-wide callback, never the commands buffered ahead of it.
   */
  @Test
  void parkWorkerAbandoningTheBatchNotifiesEveryBufferedCommand() throws Exception {
    database.async().setParallelLevel(1);
    database.async().setCommitEvery(1_000);

    final AtomicInteger executorWideErrors = new AtomicInteger();
    database.async().onError(e -> executorWideErrors.incrementAndGet());

    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK =
        callNumber -> {
          throw new ConcurrentModificationException("simulated conflict on the park worker's own commit");
        };

    final List<Exception> perCommandErrors = new CopyOnWriteArrayList<>();

    final CountDownLatch parked  = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    try {
      for (int i = 0; i < 3; i++) {
        final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
          }

          @Override
          public void onError(final Exception e) {
            perCommandErrors.add(e);
          }
        };
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
      }

      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
      // getBestSlot() rather than a hardcoded slot: with a single worker it is always slot 0, but this
      // guarantees the park task lands on the very worker the 3 commands above did, exactly like
      // quiesceWorkers() (its real caller) targets every worker by construction rather than by guessing.
      assertThat(async.scheduleTask(async.getBestSlot(), new DatabaseAsyncParkWorker(parked, release), true, 0)).isTrue();

      assertThat(parked.await(30, TimeUnit.SECONDS)).as("the park worker task must report parked").isTrue();
    } finally {
      release.countDown(); // let the parked worker resume so waitCompletion() below does not hang
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
    }

    database.async().waitCompletion();

    assertThat(executorWideErrors.get()).as("the executor-wide callback still fires, unchanged").isGreaterThanOrEqualTo(1);

    // The actual fix: all 3 commands buffered ahead of the park task are told their own onError too.
    assertThat(perCommandErrors).hasSize(3);
    for (final Exception e : perCommandErrors)
      assertThat(e).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
  }

  /**
   * Code review flagged one more site: {@code AsyncThread#executeTask}'s own generic catch, reached whenever a
   * task's {@code execute()} lets an exception escape uncaught - several task types (e.g. the graph edge-creation
   * ones) have no internal try/catch at all. Before this, that rollback destroyed every command already buffered in
   * {@code pendingBatchCommands} from earlier in the same batch with nobody but the executor-wide callback ever
   * told - the exact "already said success, then silently lost" shape #7615 is about, just triggered by a sibling
   * task's own failure instead of a periodic-boundary commit conflict.
   */
  @Test
  void aSiblingTaskThrowingUncaughtStillNotifiesEveryEarlierBufferedCommand() throws Exception {
    database.async().setParallelLevel(1); // deterministic: the failing task must land on the same worker
    database.async().setCommitEvery(1_000); // large enough that only the sibling task's failure is in play

    final AtomicInteger executorWideErrors = new AtomicInteger();
    database.async().onError(e -> executorWideErrors.incrementAndGet());

    final List<Exception> perCommandErrors = new CopyOnWriteArrayList<>();

    for (int i = 0; i < 3; i++) {
      final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
        }

        @Override
        public void onError(final Exception e) {
          perCommandErrors.add(e);
        }
      };
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
    }

    // A minimal stand-in for CreateEdgeAsyncTask and its siblings: no try/catch of its own, exactly the shape
    // that reaches executeTask()'s generic catch rather than any of this PR's own notify/retry machinery.
    final DatabaseAsyncTask throwingTask = new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
        throw new ConcurrentModificationException("simulated conflict with no local handling at all");
      }
    };

    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
    assertThat(async.scheduleTask(async.getBestSlot(), throwingTask, true, 0)).isTrue();

    database.async().waitCompletion();

    assertThat(executorWideErrors.get()).as("the executor-wide callback still fires, unchanged").isGreaterThanOrEqualTo(1);

    // The actual fix: all 3 commands buffered ahead of the throwing task are told their own onError too.
    assertThat(perCommandErrors).hasSize(3);
    for (final Exception e : perCommandErrors)
      assertThat(e).isInstanceOf(ConcurrentModificationException.class);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
  }

  /**
   * Code review's second, more commonly-triggered gap: a command that fails and rolls back the shared batch
   * <em>locally</em> - {@link DatabaseAsyncCommand#execute} catching and handling its own failure, never
   * propagating to {@code executeTask()} - used to notify only itself, not the sibling commands buffered earlier
   * in the same now-discarded batch. Unlike every other scenario this PR fixes, this one needs no fault-injection
   * hook at all: an ordinary bad SQL statement is enough, which is exactly what makes it the more likely trigger
   * in practice than a periodic-boundary MVCC conflict.
   */
  @Test
  void aSiblingCommandFailingLocallyStillNotifiesEveryEarlierBufferedCommand() throws Exception {
    database.async().setParallelLevel(1); // deterministic: the failing command must land on the same worker
    database.async().setCommitEvery(1_000); // large enough that only the local failure below is in play

    final List<Exception> perCommandErrors  = new CopyOnWriteArrayList<>();
    final AtomicInteger    failingCommandErrors = new AtomicInteger();

    for (int i = 0; i < 3; i++) {
      final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
        }

        @Override
        public void onError(final Exception e) {
          perCommandErrors.add(e);
        }
      };
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", cb, i);
    }

    // Ordinary bad SQL - not a ConcurrentModificationException, not routed through any fault-injection hook -
    // caught and handled entirely inside DatabaseAsyncCommand.execute() itself.
    database.async().command("sql", "INSERT INTO " + TYPE + " NOT VALID SYNTAX AT ALL", new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
      }

      @Override
      public void onError(final Exception e) {
        failingCommandErrors.incrementAndGet();
      }
    });

    database.async().waitCompletion();

    // DatabaseAsyncCommand's own local-failure path never calls the executor-wide onError() at all (only
    // notifyError(), a per-command callback) - unchanged by this fix, unlike CreateRecord/UpdateRecord/DeleteRecord's
    // equivalent catch blocks, which do. Not asserted on here since it is not what this test is about.
    assertThat(failingCommandErrors.get()).as("the failing command's own callback still fires, unchanged").isEqualTo(1);

    // The actual fix: all 3 commands buffered ahead of the failing one are told their own onError too, instead of
    // silently losing writes whose onComplete already fired - the same shape as #7615 itself, just triggered by
    // an ordinary command failure instead of a periodic-boundary commit conflict.
    assertThat(perCommandErrors).as("every command buffered ahead of the local failure must be told too").hasSize(3);

    database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
  }
}
