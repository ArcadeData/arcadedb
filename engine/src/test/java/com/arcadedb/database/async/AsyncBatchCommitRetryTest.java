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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionRetryPolicy;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.LockManager;
import java.io.File;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for AsyncBatchCommitRetry. Each nested class keeps the fixture of the test it was folded from.
 */
class AsyncBatchCommitRetryTest {

  /**
   * Issue #10759: {@code db.async().transaction(block, retries)} and the async executor's batch commit retried only a
   * {@link ConcurrentModificationException}, although both javadocs promise the whole {@link NeedRetryException} family, which
   * the synchronous loop, the remote one and {@code COMMIT RETRY} honour. A {@link LockTimeoutException} from the commit's own
   * file locks - what N async workers committing into the same buckets produce - failed to the caller on the first attempt,
   * and a batch commit abandoned every buffered command after one wait.
   * <p>
   * The family is enumerated from the source of truth, the {@link NeedRetryException} subclasses of the exception package,
   * not from a hand-written list, and each member is driven through every engine retry loop, which must make the attempts it
   * was asked for: so a new retryable exception cannot be retryable in one loop and not in another. A test throwing only a
   * {@link ConcurrentModificationException} passes against the defect, which is how it shipped.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RetryFamilyEveryLoop extends TestHelper {
    private static final String TYPE     = "Issue10759Doc";
    private static final String FUNCTION = "issue10759Throw";
    private static final int    ATTEMPTS = 4;

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE);
      // THE BACKOFF IS NOT WHAT THIS TEST IS ABOUT: NO WAIT BETWEEN ATTEMPTS
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
    }

    @Override
    protected void endTest() {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
    }

    @Test
    void theFamilyIsEnumeratedFromTheExceptionPackage() throws Exception {
      final List<Class<?>> family = new ArrayList<>();
      for (final Supplier<RuntimeException> member : retryableFamily()) {
        final RuntimeException instance = member.get();
        family.add(instance.getClass());
        assertThat(TransactionRetryPolicy.isRetryable(instance)).as(instance.getClass().getName()).isTrue();
      }
      assertThat(family).as("the scan must find the members the loops have to retry, or every other test proves nothing")
          .contains(NeedRetryException.class, ConcurrentModificationException.class, LockTimeoutException.class,
              DuplicatedKeyException.class);
    }

    @Test
    void synchronousTransactionRetriesEveryMember() throws Exception {
      for (final Supplier<RuntimeException> member : retryableFamily()) {
        final AtomicInteger attempts = new AtomicInteger();
        final RuntimeException thrown = member.get();
        assertThatThrownBy(() -> database.transaction(() -> {
          attempts.incrementAndGet();
          throw member.get();
        }, false, ATTEMPTS)).isInstanceOf(thrown.getClass());
        assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, ATTEMPTS));
      }
    }

    @Test
    void asyncTransactionRetriesEveryMember() throws Exception {
      for (final Supplier<RuntimeException> member : retryableFamily()) {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicReference<Throwable> reported = new AtomicReference<>();
        final RuntimeException thrown = member.get();
        database.async().transaction(() -> {
          attempts.incrementAndGet();
          throw member.get();
        }, ATTEMPTS, null, reported::set);
        database.async().waitCompletion();

        // THE ASYNC LOOP TAKES ITS ARGUMENT AS RETRIES: ONE ATTEMPT MORE THAN THE SYNCHRONOUS ONE
        assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, ATTEMPTS + 1));
        assertThat(reported.get()).as("the caller is told, once the attempts are spent").isInstanceOf(thrown.getClass());
      }
    }

    @Test
    void asyncBatchCommitRetriesEveryMember() throws Exception {
      final int maxRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
      database.async().setParallelLevel(1);
      // FAR ABOVE THE TWO COMMANDS: THE ONE BATCH COMMIT IS THE FLUSH OF waitCompletion()
      database.async().setCommitEvery(1_000);
      try {
        for (final Supplier<RuntimeException> member : retryableFamily()) {
          final AtomicInteger attempts = new AtomicInteger();
          final AtomicInteger errors = new AtomicInteger();
          final RuntimeException thrown = member.get();
          DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
            attempts.incrementAndGet();
            throw member.get();
          };
          try {
            // A BATCH OF REPLAYABLE COMMANDS, THE SHAPE THE REPLAY RETRIES
            for (int i = 0; i < 2; i++)
              database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = :seq", new AsyncResultsetCallback() {
                @Override
                public void onComplete(final ResultSet rs) {
                }

                @Override
                public void onError(final Exception e) {
                  errors.incrementAndGet();
                }
              }, Map.of("seq", i));
            database.async().waitCompletion();
          } finally {
            DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
          }

          assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, maxRetries + 1));
          assertThat(errors.get()).as("every buffered command hears of the abandoned batch").isEqualTo(2);
        }
      } finally {
        database.async().waitCompletion();
      }
      assertThat(database.countType(TYPE, true)).isZero();
    }

    @Test
    void commitRetryRetriesEveryMember() throws Exception {
      final AtomicInteger attempts = new AtomicInteger();
      final AtomicReference<Supplier<RuntimeException>> toThrow = new AtomicReference<>();
      final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
      sql.getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
        @Override
        public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
            final CommandContext context) {
          attempts.incrementAndGet();
          throw toThrow.get().get();
        }

        @Override
        public String getSyntax() {
          return FUNCTION + "()";
        }
      });
      try {
        for (final Supplier<RuntimeException> member : retryableFamily()) {
          attempts.set(0);
          toThrow.set(member);
          final RuntimeException thrown = member.get();
          assertThatThrownBy(() -> database.command("sqlscript", "BEGIN; SELECT " + FUNCTION + "(); COMMIT RETRY " + ATTEMPTS + ";"))
              .isInstanceOf(thrown.getClass());
          assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, ATTEMPTS));
          if (database.isTransactionActive())
            database.rollback();
        }
      } finally {
        sql.getFunctionFactory().unregister(FUNCTION);
      }
    }

    /**
     * The reporter's probe on the real path, not a thrown stand-in: another holder owns the bucket's file lock when the async
     * transaction commits, so the first commit times out on the lock with a {@link LockTimeoutException}. The lock is released
     * when the second attempt starts, and the retry the caller asked for commits.
     */
    @Test
    void asyncTransactionRetriesARealCommitLockTimeout() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);
      try {
        final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
        final Object holder = new Object();
        final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
        assertThat(local.getTransactionManager().tryLockFile(bucket.getFileId(), 0, holder)).isEqualTo(LockManager.LOCK_STATUS.YES);

        final AtomicInteger attempts = new AtomicInteger();
        final AtomicReference<Throwable> reported = new AtomicReference<>();
        final AtomicInteger committed = new AtomicInteger();
        try {
          database.async().transaction(() -> {
            if (attempts.incrementAndGet() == 2)
              local.getTransactionManager().unlockFile(bucket.getFileId(), holder);
            database.newDocument(TYPE).set("seq", 1).save();
          }, 3, committed::incrementAndGet, reported::set);
          database.async().waitCompletion();
        } finally {
          if (attempts.get() < 2)
            local.getTransactionManager().unlockFile(bucket.getFileId(), holder);
        }

        assertThat(reported.get()).as("the lock timeout of the first attempt must be retried, not reported").isNull();
        assertThat(attempts.get()).isEqualTo(2);
        assertThat(committed.get()).isEqualTo(1);
        assertThat(database.countType(TYPE, true)).isEqualTo(1);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getDefValue());
      }
    }

    /** A duplicated key gets one retry in every loop (#4959); every other member of the family uses every attempt. */
    private static int expectedAttempts(final RuntimeException member, final int attempts) {
      return member instanceof DuplicatedKeyException ? 2 : attempts;
    }

    /**
     * Every concrete {@link NeedRetryException} of the exception package - the package the engine raises them from - and
     * {@link DuplicatedKeyException}, as factories of a fresh instance.
     */
    private static List<Supplier<RuntimeException>> retryableFamily() throws Exception {
      final File dir = new File(NeedRetryException.class.getResource("NeedRetryException.class").toURI()).getParentFile();
      final String pkg = NeedRetryException.class.getPackageName();
      final List<Supplier<RuntimeException>> family = new ArrayList<>();
      for (final String name : dir.list()) {
        if (!name.endsWith(".class") || name.contains("$"))
          continue;
        final Class<?> c = Class.forName(pkg + "." + name.substring(0, name.length() - ".class".length()));
        if (NeedRetryException.class.isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers()))
          family.add(() -> instance(c));
      }
      family.add(() -> new DuplicatedKeyException("Issue10759Index", "[1]", new RID(1, 0)));
      return family;
    }

    private static RuntimeException instance(final Class<?> c) {
      if (c == DuplicatedKeyException.class)
        return new DuplicatedKeyException("Issue10759Index", "[1]", new RID(1, 0));
      try {
        return (RuntimeException) c.getConstructor(String.class).newInstance("simulated " + c.getSimpleName());
      } catch (final ReflectiveOperationException e) {
        throw new AssertionError(c.getName() + " needs a (String) constructor for this census", e);
      }
    }
  }

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
  @Nested
  class AsyncCommandBatchCommitConflict extends TestHelper {

    private static final String TYPE = "Issue7615Item";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE);
    }

    /**
     * A per-command-index error tracker (CodeRabbit review): an aggregate {@code hasSize(N)} plus an
     * {@code instanceof} check on every entry cannot tell a correct notify loop from a buggy one that
     * double-notifies one command and skips another entirely - both produce N entries, all
     * {@link ConcurrentModificationException}. Tracked by index instead, and verified below with
     * {@link #assertEachCommandNotifiedExactlyOnce}.
     */
    private static AsyncResultsetCallback errorTrackingCallback(final int index, final Map<Integer, AtomicInteger> errorCountsByIndex) {
      return new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
        }

        @Override
        public void onError(final Exception e) {
          assertThat(e).isInstanceOf(ConcurrentModificationException.class);
          errorCountsByIndex.computeIfAbsent(index, k -> new AtomicInteger()).incrementAndGet();
        }
      };
    }

    private static void assertEachCommandNotifiedExactlyOnce(final Map<Integer, AtomicInteger> errorCountsByIndex, final int total) {
      for (int i = 0; i < total; i++) {
        final AtomicInteger count = errorCountsByIndex.get(i);
        assertThat(count == null ? 0 : count.get())
            .as("command %d must be notified exactly once, not skipped or duplicated", i).isEqualTo(1);
      }
      assertThat(errorCountsByIndex).as("no notification for an index outside the buffered range").hasSize(total);
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

      final Map<Integer, AtomicInteger> errorCountsByIndex = new ConcurrentHashMap<>();
      final Map<Integer, AtomicInteger> okCountsByIndex     = new ConcurrentHashMap<>();

      try {
        for (int i = 0; i < 5; i++) {
          final int idx = i;
          final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
            @Override
            public void onComplete(final ResultSet rs) {
              okCountsByIndex.computeIfAbsent(idx, k -> new AtomicInteger()).incrementAndGet();
            }

            @Override
            public void onError(final Exception e) {
              assertThat(e).isInstanceOf(ConcurrentModificationException.class);
              errorCountsByIndex.computeIfAbsent(idx, k -> new AtomicInteger()).incrementAndGet();
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
      assertEachCommandNotifiedExactlyOnce(okCountsByIndex, 5);

      // The actual fix: every one of the 5 buffered commands is ALSO told its own onError once the retries
      // are exhausted, instead of the failure being visible only through the executor-wide callback above -
      // in addition to the onComplete above, which is the point (see commitBatch()'s own javadoc).
      assertEachCommandNotifiedExactlyOnce(errorCountsByIndex, 5);

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

      final Map<Integer, AtomicInteger> errorCountsByIndex = new ConcurrentHashMap<>();

      try {
        for (int i = 0; i < 3; i++)
          database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", errorTrackingCallback(i, errorCountsByIndex), i);

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
      assertEachCommandNotifiedExactlyOnce(errorCountsByIndex, 3);

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

      final Map<Integer, AtomicInteger> errorCountsByIndex = new ConcurrentHashMap<>();

      final CountDownLatch parked  = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);

      try {
        for (int i = 0; i < 3; i++)
          database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", errorTrackingCallback(i, errorCountsByIndex), i);

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
      assertEachCommandNotifiedExactlyOnce(errorCountsByIndex, 3);

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

      final Map<Integer, AtomicInteger> errorCountsByIndex = new ConcurrentHashMap<>();

      for (int i = 0; i < 3; i++)
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", errorTrackingCallback(i, errorCountsByIndex), i);

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
      assertEachCommandNotifiedExactlyOnce(errorCountsByIndex, 3);

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

      final Map<Integer, AtomicInteger> errorCountsByIndex   = new ConcurrentHashMap<>();
      final AtomicInteger               failingCommandErrors = new AtomicInteger();

      for (int i = 0; i < 3; i++) {
        final int idx = i;
        final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
          }

          @Override
          public void onError(final Exception e) {
            errorCountsByIndex.computeIfAbsent(idx, k -> new AtomicInteger()).incrementAndGet();
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
      assertEachCommandNotifiedExactlyOnce(errorCountsByIndex, 3);

      database.transaction(() -> assertThat(database.countType(TYPE, true)).isZero());
    }

    /**
     * The code review found that {@code pendingUnreplayableTasks} classified purely by {@code requiresActiveTx()},
     * which answers "does this task need {@code begin()} called first," not "did this task write something
     * {@code commitBatch()} cannot replay." A pure read like {@link DatabaseAsyncScanBucket} (dispatched via
     * {@code scanType()}) defaults {@code requiresActiveTx() == true} while writing nothing at all - landing mid-batch
     * on a worker with buffered commands used to disable that worker's retry-by-replay for no reason, falling back to
     * abandon-and-notify for a conflict a replay could have resolved transparently. Not a data-loss regression (every
     * buffered command was still notified either way), but it defeated the retry this PR exists to add.
     */
    @Test
    void aPureReadTaskInterleavedMidBatchDoesNotDisableTheRetry() throws Exception {
      database.async().setParallelLevel(1); // deterministic: the scan must land on the same worker as the commands
      database.async().setCommitEvery(1_000); // large enough that only waitCompletion()'s tail flush is in play

      final AtomicInteger hookCalls = new AtomicInteger();
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
        if (hookCalls.incrementAndGet() == 1)
          throw new ConcurrentModificationException("simulated conflict at the first attempt only");
      };

      final Map<Integer, AtomicInteger> errorCountsByIndex = new ConcurrentHashMap<>();

      try {
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", errorTrackingCallback(0, errorCountsByIndex), 0);
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", errorTrackingCallback(1, errorCountsByIndex), 1);

        // A pure read landing mid-batch on the very same worker (parallel level 1) - blocks until the scan
        // completes, exactly like the real interleaving a concurrent scanType()/parallel SELECT would produce.
        database.async().scanType(TYPE, false, record -> true);

        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = ?", errorTrackingCallback(2, errorCountsByIndex), 2);

        database.async().waitCompletion();
      } finally {
        DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
      }

      // The actual fix: the scan does not count as an unreplayable write, so the conflict on the first attempt
      // is retried by replaying the 3 buffered commands instead of being abandoned outright - a second hook call
      // is proof a retry was actually attempted, not just that the (still correct either way) notification fired.
      assertThat(hookCalls.get()).as("the conflict must be retried, not abandoned on the first attempt").isGreaterThanOrEqualTo(2);
      assertThat(errorCountsByIndex).as("no command should need its own onError - the retry must succeed transparently").isEmpty();

      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(3));
    }
  }

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
  @Nested
  class AsyncMidStatementCommitReplay extends TestHelper {

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

  /**
   * Regression for issue #7673: three of the six sites that durably commit an async worker's shared batch never fired
   * the executor-wide {@code onOk()} that #6470 established as THE durability signal for that batch.
   * <p>
   * {@code commitBatch()} (the periodic {@code commitEvery} boundary), {@code closeTransactionBoundaryIfDurability
   * PolicyChanged()} and the worker's shutdown commit all do {@code commit(); onOk(); clearBatchState();}. The three
   * out-of-band sites - {@link DatabaseAsyncIndexCompaction}, {@link DatabaseAsyncParkWorker} and
   * {@link DatabaseAsyncTransaction}'s flush of a dangling prior batch - committed and cleared, but skipped the
   * signal, so a caller counting durable batches (the use case #6470 was filed for) silently missed them.
   * <p>
   * Each test below submits writes that stay in the worker's still-open batch (a {@code commitEvery} far above the
   * number submitted, so no periodic boundary can fire), triggers ONE of the three paths, and asserts that
   * {@code onOk()} arrived. Deliberately asserted BEFORE {@code waitCompletion()}: the completion marker and the
   * shutdown commit both fire {@code onOk()} on their own, so a test that only counted at the end would pass against
   * the unfixed code.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncOutOfBandCommitDurabilitySignal extends TestHelper {

    private static final String TYPE = "Issue7673Item";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("seq", Type.INTEGER);
    }

    /**
     * {@code db.async().transaction(...)} landing on a worker that still holds an uncommitted batch flushes that
     * batch first. That flush is a real, durable commit of somebody else's writes and must say so.
     */
    @Test
    void asyncTransactionFlushOfADanglingBatchFiresTheDurabilitySignal() throws Exception {
      final CountDownLatch signalled = new CountDownLatch(1);
      final AtomicInteger  onOkCount = prepareBatch(signalled);

      final CountDownLatch txDone = new CountDownLatch(1);
      database.async().transaction(txDone::countDown);

      assertThat(txDone.await(30, TimeUnit.SECONDS)).as("the async transaction ran").isTrue();
      assertThat(signalled.await(30, TimeUnit.SECONDS))
          .as("the flush of the dangling batch durably committed it, so onOk() must have fired (#6470/#7673)").isTrue();
      assertThat(onOkCount.get()).isPositive();
    }

    /**
     * Quiescing the workers commits each one's open batch before parking it - the commit is half the point of
     * {@link DatabaseAsyncParkWorker} (issue #6281), and it is just as durable as the periodic one.
     */
    @Test
    void parkingAWorkerFiresTheDurabilitySignalForTheBatchItPublishes() throws Exception {
      final CountDownLatch signalled = new CountDownLatch(1);
      final AtomicInteger  onOkCount = prepareBatch(signalled);

      try (final AsyncQuiesce ignored = ((DatabaseInternal) database).quiesceAsync()) {
        assertThat(signalled.await(30, TimeUnit.SECONDS))
            .as("the park worker durably committed the batch, so onOk() must have fired (#6470/#7673)").isTrue();
      }
      assertThat(onOkCount.get()).isPositive();
    }

    /**
     * An index compaction task landing mid-batch commits whatever the worker had already run against its shared
     * transaction before it can compact. Same durable commit, same signal.
     */
    @Test
    void indexCompactionFiresTheDurabilitySignalForTheBatchItPublishes() throws Exception {
      final IndexInternal index = (IndexInternal) database.getSchema()
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "seq");

      final CountDownLatch signalled = new CountDownLatch(1);
      final AtomicInteger  onOkCount = prepareBatch(signalled);

      // The task itself rather than compact(index): scheduleCompaction()'s AVAILABLE -> COMPACTION_SCHEDULED
      // reservation is about throttling repeated compactions, and this test is about the commit that precedes one.
      ((DatabaseAsyncExecutorImpl) database.async())
          .scheduleTask(0, new DatabaseAsyncIndexCompaction(index), true, 0);

      assertThat(signalled.await(30, TimeUnit.SECONDS))
          .as("the compaction task durably committed the pending batch, so onOk() must have fired (#6470/#7673)")
          .isTrue();
      assertThat(onOkCount.get()).isPositive();
    }

    /**
     * One worker holding an open batch of three writes, a {@code commitEvery} no periodic boundary can reach, and an
     * {@code onOk()} listener that has not been told anything yet.
     *
     * @param signalled counted down by the listener the first time it fires
     *
     * @return the listener's invocation count, guaranteed still zero when this returns
     */
    private AtomicInteger prepareBatch(final CountDownLatch signalled) throws InterruptedException {
      database.async().setParallelLevel(1);
      database.async().setCommitEvery(10_000);

      final CountDownLatch applied = new CountDownLatch(3);
      for (int i = 0; i < 3; i++)
        database.async().createRecord(database.newDocument(TYPE).set("seq", i), r -> applied.countDown());

      assertThat(applied.await(30, TimeUnit.SECONDS)).as("the three writes reached the worker's open batch").isTrue();

      // Registered only now, so nothing that happened while the batch was filling can count towards the assertion.
      final AtomicInteger onOkCount = new AtomicInteger();
      database.async().onOk(() -> {
        onOkCount.incrementAndGet();
        signalled.countDown();
      });
      return onOkCount;
    }
  }

  /**
   * Regression for issue #9529: the replay {@code commitBatch()} runs after a {@link ConcurrentModificationException}
   * (issue #7615) used to start immediately, so with several workers appending to the tail page of one bucket the
   * replay raced the very workers that had just caused the conflict, and about 1 run in 100 on two cores ran out of
   * {@link GlobalConfiguration#TX_RETRIES}. Like {@code LocalDatabase#transaction}, the replay now waits the jittered
   * exponential backoff of {@link GlobalConfiguration#TX_RETRY_DELAY} / {@link GlobalConfiguration#TX_RETRY_DELAY_BASE}
   * (issue #5587) before every attempt after the first.
   * <p>
   * The wait is observed through {@link DatabaseAsyncExecutorImpl#TEST_BATCH_RETRY_DELAY_HOOK} rather than timed: the
   * delay is a random draw, and a wall-clock bound would only measure the JVM's mood.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncBatchReplayBackoff extends TestHelper {

    private static final String TYPE = "Issue9529Item";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE);
    }

    private void insert(final int total) {
      final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
        }

        @Override
        public void onError(final Exception e) {
        }
      };
      for (int i = 0; i < total; i++)
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = :seq", cb, Map.of("seq", i));
    }

    @Test
    void everyReplayWaitsABackoffWithinItsWindowBeforeRunning() throws Exception {
      final int total = 10;
      final int base = 40;
      final int cap = 100;

      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, base);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, cap);

      database.async().setParallelLevel(1);
      database.async().setCommitEvery(total);
      final AtomicInteger errors = new AtomicInteger();
      database.async().onError(e -> errors.incrementAndGet());

      final List<Long> delays = new CopyOnWriteArrayList<>();
      final AtomicInteger commitCalls = new AtomicInteger();
      DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = delays::add;
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
        if (commitCalls.incrementAndGet() <= 2)
          throw new ConcurrentModificationException("simulated conflict " + callNumber);
      };

      try {
        insert(total);
        database.async().waitCompletion();
      } finally {
        DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
        DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = null;
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, GlobalConfiguration.TX_RETRY_DELAY_BASE.getDefValue());
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
      }

      assertThat(errors.get()).as("two conflicts are within the default retries").isZero();
      assertThat(delays).as("one backoff per replay, none before the first attempt").hasSize(2);
      // retry 0 draws from [1, base], retry 1 from [1, min(cap, 2 * base)]
      assertThat(delays.get(0)).isBetween(1L, (long) base);
      assertThat(delays.get(1)).isBetween(1L, (long) Math.min(cap, 2 * base));
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(total));
    }

    @Test
    void noBackoffWithoutConflict() throws Exception {
      database.async().setParallelLevel(1);
      database.async().setCommitEvery(5);

      final AtomicInteger delays = new AtomicInteger();
      DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = delay -> delays.incrementAndGet();
      try {
        insert(23);
        database.async().waitCompletion();
      } finally {
        DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = null;
      }

      assertThat(delays.get()).isZero();
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(23));
    }

    @Test
    void zeroRetryDelayDisablesTheBackoff() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);

      database.async().setParallelLevel(1);
      database.async().setCommitEvery(5);

      final AtomicInteger delays = new AtomicInteger();
      final AtomicInteger commitCalls = new AtomicInteger();
      DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = delay -> delays.incrementAndGet();
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
        if (commitCalls.incrementAndGet() == 1)
          throw new ConcurrentModificationException("simulated conflict");
      };

      try {
        insert(5);
        database.async().waitCompletion();
      } finally {
        DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
        DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = null;
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
      }

      assertThat(delays.get()).isZero();
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(5));
    }

    /**
     * The scenario of the report: four workers sharing the tail page of one bucket. Probabilistic by nature (the
     * failure was ~1% on two cores before the fix), so it is a guard against the replay racing its own cause, not a
     * proof - the hook-driven tests above are the deterministic ones.
     */
    @Test
    void fourWorkersOnOneBucketLoseNoBatch() throws Exception {
      final int total = 9_742;

      database.async().setParallelLevel(4);
      database.async().setCommitEvery(1_000);
      final AtomicInteger errors = new AtomicInteger();
      database.async().onError(e -> errors.incrementAndGet());

      insert(total);
      database.async().waitCompletion();

      assertThat(errors.get()).isZero();
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(total));
    }
  }

  /**
   * Found on PR #10121 (issue #9575): an {@link Error} thrown inside an async transaction, an {@link AssertionError} or an
   * {@link OutOfMemoryError} on a large load, was caught by the worker loop and rolled back, but never reached the task's own
   * error callback. A caller that waits for completion and then asks its callback what failed - the parallel flush of a
   * GraphBatch - took the task for one that had committed, and left the edges of that bucket unconnected without a word.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncTransactionErrorReachesCallback extends TestHelper {

    @Test
    void anErrorInAnAsyncTransactionReachesItsErrorCallbackAndRollsBack() {
      final AtomicBoolean okCalled = new AtomicBoolean();
      final AtomicReference<Throwable> taskError = new AtomicReference<>();

      database.async().transaction(() -> {
            database.newDocument("Doc").set("id", 1).save();
            throw new AssertionError("simulated");
          }, 0, () -> okCalled.set(true), taskError::set);
      database.async().waitCompletion();

      assertThat(okCalled.get()).isFalse();
      assertThat(taskError.get()).isInstanceOf(AssertionError.class).hasMessage("simulated");
      assertThat(database.countType("Doc", false)).as("documents left by the failed transaction").isZero();
    }

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Doc");
    }
  }

  /**
   * Regression tests for #6470: an async {@code createRecord}/{@code updateRecord}/{@code deleteRecord}'s own success
   * callback (e.g. {@link NewRecordCallback}) fires as soon as the write is applied to the worker's current batch
   * transaction, which is not necessarily durable yet - a later sibling failure in the same still-open batch can still
   * roll the whole thing back, discarding a write whose callback already fired.
   * <p>
   * That per-record timing is unchanged by this fix (and is exercised, unchanged, by
   * {@code Issue6281AsyncBatchIndexBuildTest} and others that rely on it as a "this task has run" signal rather than a
   * durability signal - changing it broke that suite). What was missing, and what {@code Issue6470} is actually about,
   * is a reliable way to know a write IS durable: {@link DatabaseAsyncExecutor#onOk(OkCallback)} - previously fired
   * only at executor shutdown and from the {@code waitCompletion()} marker - now also fires at the periodic
   * {@code setCommitEvery(int)} boundary and at a durability-setting boundary commit, so it is a complete signal for
   * every point this shared batch can actually commit.
   */
  @Nested
  class AsyncSuccessCallbackDurability extends TestHelper {

    private static final String TYPE = "Issue6470Item";

    /**
     * Documents the (intentional, unchanged) provisional nature of the per-record callback: it fires for the 1st and
     * 2nd creates below even though the 3rd's failure rolls the whole still-open batch back and discards them.
     */
    @Test
    void perRecordCallbackFiresBeforeTheBatchIsNecessarilyDurable() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1));

      final AtomicInteger errors    = new AtomicInteger();
      final List<RID>     confirmed = new CopyOnWriteArrayList<>();

      // Keyed off content rather than a raw listener-call count, so the trigger is unambiguous regardless of how many
      // times the engine happens to invoke an after-create listener per create.
      final AfterRecordCreateListener throwOnMarked = record -> {
        if ("fail".equals(((Document) record).getString("marker")))
          throw new RuntimeException("injected error on third create");
      };

      database.async().setCommitEvery(10);
      database.async().onError(e -> errors.incrementAndGet());

      database.getEvents().registerListener(throwOnMarked);
      try {
        // 1st and 2nd creates succeed and stay in the still-open batch; the 3rd's listener throws, rolling the whole
        // batch back (1st and 2nd included); the 4th runs in the fresh transaction begun after that rollback.
        database.async().createRecord(database.newDocument(TYPE).set("marker", "ok"), r -> confirmed.add(r.getIdentity()));
        database.async().createRecord(database.newDocument(TYPE).set("marker", "ok"), r -> confirmed.add(r.getIdentity()));
        database.async().createRecord(database.newDocument(TYPE).set("marker", "fail"), r -> confirmed.add(r.getIdentity()));
        database.async().createRecord(database.newDocument(TYPE).set("marker", "ok"), r -> confirmed.add(r.getIdentity()));

        database.async().waitCompletion();
      } finally {
        database.getEvents().unregisterListener(throwOnMarked);
      }

      assertThat(errors.get()).isEqualTo(1);

      // By design: the per-record callback already fired for the 1st and 2nd creates too, even though the 3rd's
      // failure rolled the whole still-open batch back and discarded them - it reports "applied", not "durable".
      assertThat(confirmed).hasSize(3);

      // Only the 4th create - the one in the fresh transaction begun after the rollback - actually exists.
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(1));
    }

    /**
     * The actual fix: {@code onOk()} now fires at the periodic {@code commitEvery} boundary, so a caller has a
     * reliable durability signal without needing to wait for the executor to go idle or shut down.
     */
    @Test
    void executorWideOnOkFiresAtThePeriodicCommitBoundary() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1));

      database.async().setCommitEvery(5);

      final CountDownLatch firstBoundaryCommitted = new CountDownLatch(1);
      final AtomicInteger  onOkInvocations        = new AtomicInteger();
      database.async().onOk(() -> {
        onOkInvocations.incrementAndGet();
        firstBoundaryCommitted.countDown();
      });

      for (int i = 0; i < 12; i++)
        database.async().createRecord(database.newDocument(TYPE), null);

      // Proves the periodic commitEvery boundary itself signals durability - not just the final waitCompletion()
      // marker, which has not been called yet at this point.
      assertThat(firstBoundaryCommitted.await(30, TimeUnit.SECONDS))
          .as("onOk() must fire at the periodic commitEvery boundary, before waitCompletion() is even called")
          .isTrue();

      database.async().waitCompletion();

      // 12 creates over a batch of 5 cross the periodic boundary twice (at the 5th and 10th), so at least 2 by the
      // time the loop above already observed 1 of them. (waitCompletion()'s own completion marker also triggers onOk()
      // on every worker, including idle ones, so the final count is not asserted precisely here.)
      assertThat(onOkInvocations.get()).isGreaterThanOrEqualTo(2);

      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(12));
    }

    /**
     * The other real commit point {@code onOk()} was extended to cover: a durability-setting change
     * ({@code setTransactionSync}/{@code setTransactionUseWAL}) mid-batch forces the worker to commit its
     * already-open transaction under its OLD flags before applying the new ones - see
     * {@code DatabaseAsyncExecutorImpl.closeTransactionBoundaryIfDurabilityPolicyChanged()}.
     */
    @Test
    void executorWideOnOkFiresAtTheDurabilityPolicyBoundary() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1));
      // Large enough that the periodic commitEvery boundary this test is NOT about never fires here.
      database.async().setCommitEvery(100);

      final AtomicInteger  onOkInvocations = new AtomicInteger();
      final CountDownLatch fired           = new CountDownLatch(1);
      database.async().onOk(() -> {
        onOkInvocations.incrementAndGet();
        fired.countDown();
      });

      // Single bucket, so both creates land on the same worker in submission order: the 1st opens a
      // transaction stamped with the CURRENT durability flags and leaves it open (commitEvery is nowhere
      // near reached); the flag flip in between is picked up only when the 2nd create is dispatched, at
      // which point that still-open transaction's stamped flags no longer match and must be committed first.
      database.async().createRecord(database.newDocument(TYPE), null);
      database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
      database.async().createRecord(database.newDocument(TYPE), null);

      assertThat(fired.await(30, TimeUnit.SECONDS))
          .as("onOk() must fire when a durability-setting change forces a boundary commit mid-batch")
          .isTrue();

      database.async().waitCompletion();
      assertThat(onOkInvocations.get()).isGreaterThanOrEqualTo(1);
      database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(2));
    }
  }
}
