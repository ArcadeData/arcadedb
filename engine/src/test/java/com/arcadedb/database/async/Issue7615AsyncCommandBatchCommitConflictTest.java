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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
}
