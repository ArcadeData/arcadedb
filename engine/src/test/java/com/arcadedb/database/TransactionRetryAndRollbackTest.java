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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the database engine: TransactionRetryAndRollback. Each nested class keeps the regression of one reported issue (see the
 * Javadoc of each).
 */
public class TransactionRetryAndRollbackTest {
  /**
   * Regression test for #5587: before this fix, every {@code database.transaction(...)} retry slept a flat
   * {@code U(1, TX_RETRY_DELAY)} interval no matter how many times the transaction had already lost, so a
   * transaction on attempt 50 had the same odds of winning the next race as one on attempt 1. This proves the
   * per-attempt window now widens (exponential backoff with full jitter,
   * {@code min(TX_RETRY_DELAY, TX_RETRY_DELAY_BASE * 2^attempt)}) instead of staying flat.
   * <p>
   * The {@code error} callback fires once per failed attempt, right before the backoff sleep, so the wall-clock
   * gap between consecutive callback invocations is (up to negligible retry-loop overhead) that attempt's sleep.
   * Comparing the sum of the first few gaps against the sum of the last few isolates the backoff's growth from
   * the retry loop's own cost, the same in-run-comparison approach {@code TxRetryDelayScope} uses to
   * avoid depending on an absolute wall-clock budget.
   */
  @Nested
  class TxRetryExponentialBackoff extends TestHelper {
    /** One more than the number of forced failures, so the last attempt succeeds. */
    private static final int ATTEMPTS = 10;
    /** Small enough that early windows (2, 4, 8, 16 ms) stay tiny... */
    private static final int BASE_MS  = 2;
    /** ...and large enough that the late windows (32, 64, 128, 256 ms) never saturate against it. */
    private static final int CAP_MS   = 1_000;

    @Test
    void laterRetriesBackOffFurtherThanEarlierOnes() {
      final int savedCap = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      final int savedBase = GlobalConfiguration.TX_RETRY_DELAY_BASE.getValueAsInteger();
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, CAP_MS);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, BASE_MS);
      try {
        final List<Long> failureTimestampsNanos = new ArrayList<>();
        // #6260: a stop-the-world pause landing inside one gap would corrupt the comparison below in whichever
        // direction it fell, so each gap is measured with the JVM's own stalls taken out of it.
        final List<Long> failureStallNanos = new ArrayList<>();
        final AtomicInteger attempt = new AtomicInteger();

        database.transaction(() -> {
          if (attempt.incrementAndGet() < ATTEMPTS)
            throw new ConcurrentModificationException("forced retry");
        }, false, ATTEMPTS, null, e -> {
          failureTimestampsNanos.add(System.nanoTime());
          failureStallNanos.add(StallAwareStopwatch.jvmStallNanos());
        });

        assertThat(attempt.get()).isEqualTo(ATTEMPTS);
        // one error callback per failed attempt, i.e. every attempt but the last
        assertThat(failureTimestampsNanos).hasSize(ATTEMPTS - 1);

        final List<Long> gapsMs = new ArrayList<>();
        for (int i = 1; i < failureTimestampsNanos.size(); i++) {
          final long spanNanos = failureTimestampsNanos.get(i) - failureTimestampsNanos.get(i - 1);
          final long stalledNanos = failureStallNanos.get(i) - failureStallNanos.get(i - 1);
          gapsMs.add(Math.max(0L, spanNanos - stalledNanos) / 1_000_000);
        }

        final int half = gapsMs.size() / 2;
        final long earlyGapsSumMs = gapsMs.subList(0, half).stream().mapToLong(Long::longValue).sum();
        final long lateGapsSumMs = gapsMs.subList(gapsMs.size() - half, gapsMs.size()).stream().mapToLong(Long::longValue).sum();

        // Expected sums are ~17ms (early, windows 2/4/8/16) vs ~242ms (late, windows 32/64/128/256) - a flat
        // delay (the pre-#5587 behaviour) would make the two statistically indistinguishable instead.
        assertThat(lateGapsSumMs)
            .as("later retries (gaps %s) should back off further than earlier ones (gaps %s)", gapsMs.subList(gapsMs.size() - half, gapsMs.size()),
                gapsMs.subList(0, half))
            .isGreaterThan(earlyGapsSumMs * 2);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedCap);
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, savedBase);
      }
    }

    @Test
    void aZeroCapDisablesTheDelayEntirelyJustLikeBeforeTheFix() {
      final int savedCap = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
      try {
        final AtomicInteger attempt = new AtomicInteger();
        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

        database.transaction(() -> {
          if (attempt.incrementAndGet() < ATTEMPTS)
            throw new ConcurrentModificationException("forced retry");
        }, false, ATTEMPTS);

        assertThat(attempt.get()).isEqualTo(ATTEMPTS);
        stopwatch.assertStayedUnder(1_000, "10 retries with the delay disabled, not 10 retries that each sleep");
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedCap);
      }
    }
  }

  /**
   * TX_RETRY_DELAY has database scope, so the backoff between transaction retries must come from the database's
   * own configuration and not from the global static, which ignores any per-database override.
   * <p>
   * The assertions compare an in-run baseline (the same retry loop with no backoff configured) against the same
   * loop with the backoff raised, so nothing depends on an absolute wall-clock budget. When the retry loop reads
   * the global static the two runs are indistinguishable and the gap collapses to zero.
   * <p>
   * Since #5587, the per-attempt window is {@code min(TX_RETRY_DELAY, TX_RETRY_DELAY_BASE * 2^attempt)} instead
   * of a flat {@code TX_RETRY_DELAY}. Both settings are set to the same {@link #RAISED_DELAY_MS} here, which
   * collapses that formula back to a flat window on every attempt ({@code min(x, x * 2^n) == x}) - the scope
   * behaviour under test does not depend on the backoff shape, only on which configuration wins, so pinning the
   * shape keeps this test's timing budget identical to before #5587.
   */
  @Nested
  class TxRetryDelayScope extends TestHelper {
    /** One more than the number of forced failures, so the last attempt succeeds. */
    private static final int ATTEMPTS         = 7;
    /** Each of the {@link #ATTEMPTS} - 1 backoffs sleeps a random 1..RAISED_DELAY_MS, so the sum dwarfs the gap. */
    private static final int RAISED_DELAY_MS  = 500;
    /** Well below the (ATTEMPTS - 1) * RAISED_DELAY_MS / 2 expected sum, and unreachable without the backoff. */
    private static final int MIN_GAP_MS       = 250;

    @Test
    void theRetryLoopUsesTheDelayConfiguredOnThisDatabase() {
      final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      final int savedGlobalBase = GlobalConfiguration.TX_RETRY_DELAY_BASE.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(0);
      try {
        // BASELINE TAKEN IN-RUN: NO BACKOFF ANYWHERE, SO THIS IS THE COST OF THE RETRY LOOP ITSELF
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
        final long baselineMs = timeForcedRetries();

        // ONLY THE DATABASE IS RETUNED: THE GLOBAL STATIC STAYS AT 0
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, RAISED_DELAY_MS);
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, RAISED_DELAY_MS);
        final long observedMs = timeForcedRetries();

        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)).isEqualTo(
            RAISED_DELAY_MS);
        assertThat(observedMs - baselineMs).isGreaterThan(MIN_GAP_MS);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedGlobal);
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, savedGlobalBase);
        GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
      }
    }

    @Test
    void theGlobalDelayStillAppliesWhenTheDatabaseDoesNotOverrideIt() {
      final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      final int savedGlobalBase = GlobalConfiguration.TX_RETRY_DELAY_BASE.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(0);
      try {
        final long baselineMs = timeForcedRetries();

        // NOTHING IS SET ON THE DATABASE, SO THE GLOBAL VALUE MUST STILL BE HONOURED
        GlobalConfiguration.TX_RETRY_DELAY.setValue(RAISED_DELAY_MS);
        GlobalConfiguration.TX_RETRY_DELAY_BASE.setValue(RAISED_DELAY_MS);
        final long observedMs = timeForcedRetries();

        assertThat(observedMs - baselineMs).isGreaterThan(MIN_GAP_MS);
      } finally {
        GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
        GlobalConfiguration.TX_RETRY_DELAY_BASE.setValue(savedGlobalBase);
      }
    }

    /**
     * Runs a transaction that fails with a retryable error on every attempt but the last, and returns how long
     * the whole retry loop took in milliseconds.
     */
    private long timeForcedRetries() {
      final AtomicInteger attempt = new AtomicInteger();
      final long start = System.nanoTime();
      database.transaction(() -> {
        if (attempt.incrementAndGet() < ATTEMPTS)
          throw new ConcurrentModificationException("forced retry");
      }, false, ATTEMPTS);
      assertThat(attempt.get()).isEqualTo(ATTEMPTS);
      return (System.nanoTime() - start) / 1_000_000;
    }
  }

  /**
   * Issue #4959: low-severity transaction-layer cleanups.
   * <ul>
   * <li>A deferred update whose record was deleted by a concurrent transaction must FAIL the commit with a retryable
   * conflict instead of being silently skipped while the rest of the transaction commits (silent partial commit).</li>
   * <li>{@code transaction()} must not burn every retry attempt on a deterministic {@link DuplicatedKeyException}:
   * only a concurrency-induced duplicate can succeed on retry, and one retry is enough to disambiguate.</li>
   * <li>{@code executeLockingFiles} must lock on behalf of the current transaction's requester (thread or session),
   * so a thread acting for a session does not time out on locks its own session already holds.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TransactionCleanups extends TestHelper {

    private static final String TYPE         = "Issue4959Doc";
    private static final String TYPE_INDEXED = "Issue4959Indexed";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE);
        final DocumentType indexed = database.getSchema().createDocumentType(TYPE_INDEXED);
        indexed.createProperty("id", String.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_INDEXED, "id");
      });
    }

    @Test
    void deferredUpdateOfConcurrentlyDeletedRecordFailsTheCommit() throws Exception {
      final MutableDocument[] holder = new MutableDocument[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE);
        doc.set("name", "committed");
        doc.save();
        holder[0] = doc;
      });
      final RID rid = holder[0].getIdentity();

      database.begin();
      final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();

      // Concurrent transaction (own thread = own transaction context) deletes the record and commits. The current
      // transaction holds no locks yet, so the delete goes through.
      final Thread concurrentDelete = new Thread(
          () -> database.transaction(() -> database.deleteRecord(database.lookupByRID(rid, true))));
      concurrentDelete.start();
      concurrentDelete.join();

      // Deferred update: the page is loaded AFTER the delete committed, so the MVCC page-version check cannot see
      // the conflict. Before the fix the commit succeeded and the update was silently dropped.
      doc.set("name", "lost");
      doc.save();

      assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
      assertThat(database.isTransactionActive()).isFalse();
    }

    @Test
    void deterministicDuplicatedKeyIsRetriedAtMostOnce() {
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE_INDEXED);
        doc.set("id", "K");
        doc.save();
      });

      final AtomicInteger attemptsRun = new AtomicInteger();

      assertThatThrownBy(() -> database.transaction(() -> {
        attemptsRun.incrementAndGet();
        final MutableDocument dup = database.newDocument(TYPE_INDEXED);
        dup.set("id", "K");
        dup.save();
      }, false, 10)).isInstanceOf(DuplicatedKeyException.class);

      // A genuine duplicate fails identically every attempt: one retry is enough to disambiguate from a
      // concurrency-induced duplicate. Before the fix all 10 attempts (plus retry delays) were burned.
      assertThat(attemptsRun.get()).as("a deterministic duplicate must be retried at most once").isEqualTo(2);
    }

    @Test
    void executeLockingFilesUsesTheTransactionRequester() throws Exception {
      final int fileId = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();

      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      final Object session = new Object();

      // The session (not the thread) holds the file lock, as it happens for server-side transactions keyed by session.
      final List<Integer> locked = transactionManager.tryLockFiles(List.of(fileId), 2_000, session);
      try {
        database.begin();
        ((DatabaseInternal) database).getTransaction().setRequester(session);

        // Before the fix this timed out after 5s: the lock was requested as Thread.currentThread() while the
        // same logical owner (the session) already held it.
        final Boolean executed = ((DatabaseInternal) database).executeLockingFiles(List.of(fileId), () -> true);
        assertThat(executed).isTrue();

        // The session lock must still be held afterwards (executeLockingFiles releases only what it acquired).
        assertThat(transactionManager.tryLockFile(fileId, 100, new Object())).isEqualTo(LockManager.LOCK_STATUS.NO);
      } finally {
        database.rollback();
        transactionManager.unlockFilesInOrder(locked, session);
      }
    }
  }

  /**
   * Issue #4940: a {@code commit2ndPhase} failure that happens BEFORE the transaction is appended to the WAL must
   * restore user-held record state exactly like a phase-1 failure does ({@code rollback()}), not just release
   * resources ({@code reset()}). Otherwise records created in the failed transaction keep an optimistically-assigned
   * RID pointing at a record that was never persisted, and re-saving them in a retry silently updates a missing
   * record instead of re-inserting (the #4562 class of bug, resurfacing through the phase-2 failure path).
   * <p>
   * A failure AFTER the WAL append is intentionally NOT rolled back: the transaction is durable and recovery replays
   * it, so the assigned RIDs remain valid.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Phase2FailureRollback extends TestHelper {

    private static final String TYPE = "Issue4940Doc";

    @Override
    protected void beginTest() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE));
    }

    @Test
    void phase2FailureBeforeWalAppendResetsCreatedRecordIdentity() {
      database.begin();
      final MutableDocument doc = database.newDocument(TYPE);
      doc.set("name", "one");
      doc.save();
      assertThat(doc.getIdentity()).as("record gets an optimistic RID at creation").isNotNull();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

      // Force a phase-2 failure BEFORE the WAL append: invoking the 2nd phase without having run the 1st fails the
      // status precondition, which is one of the pre-durability failure points of commit2ndPhase (same finally block
      // as a WAL-append failure).
      final Throwable error = catchThrowable(
          () -> tx.commit2ndPhase(new TransactionContext.TransactionPhase1(null, List.of())));
      assertThat(error).isInstanceOf(TransactionException.class);
      assertThat(database.isTransactionActive()).isFalse();

      // #4940: nothing durable exists, so the failure must behave like any other failed commit: the identity is
      // provisional again and the same in-memory object can be cleanly re-inserted on retry.
      assertThat(doc.getIdentity()).as("identity must be reset after a pre-durability phase-2 failure").isNull();

      database.begin();
      doc.save();
      database.commit();

      assertThat(doc.getIdentity()).isNotNull();
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
      database.begin();
      assertThat(database.lookupByRID(doc.getIdentity(), true).asDocument().getString("name")).isEqualTo("one");
      database.commit();
    }

    @Test
    void phase2FailureBeforeWalAppendReloadsModifiedRecords() {
      final MutableDocument[] holder = new MutableDocument[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE);
        doc.set("name", "committed");
        doc.save();
        holder[0] = doc;
      });
      final RID rid = holder[0].getIdentity();

      database.begin();
      final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
      doc.set("name", "uncommitted");
      doc.save();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final Throwable error = catchThrowable(
          () -> tx.commit2ndPhase(new TransactionContext.TransactionPhase1(null, List.of())));
      assertThat(error).isInstanceOf(TransactionException.class);

      // #4940: the user-held object must be reloaded to its committed content, like rollback() does.
      assertThat(doc.getString("name")).as("modified record must be reloaded to the committed version").isEqualTo("committed");

      database.begin();
      assertThat(database.lookupByRID(rid, true).asDocument().getString("name")).isEqualTo("committed");
      database.commit();
    }

    @Test
    void phase2PublishFailureWithoutWalStillRollsBack() throws Exception {
      // With useWAL=false there is NO WAL append at all: nothing is ever durable in phase 2, so even a failure in the
      // page-publish stage (past the point where a WAL-enabled transaction would have crossed durability) must roll
      // back like any other pre-durability failure. Guards the placement of the walAppended flag: only a REAL append
      // may suppress the rollback.
      final MutableDocument[] holder = new MutableDocument[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE);
        doc.set("name", "committed");
        doc.save();
        holder[0] = doc;
      });
      final RID rid = holder[0].getIdentity();

      database.begin();
      database.setUseWAL(false);

      final MutableDocument modified = database.lookupByRID(rid, true).asDocument().modify();
      modified.set("name", "uncommitted");
      modified.save();

      final MutableDocument created = database.newDocument(TYPE);
      created.set("name", "new");
      created.save();
      assertThat(created.getIdentity()).isNotNull();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1).isNotNull();
      assertThat(phase1.result).as("useWAL=false produces no WAL buffer").isNull();

      // Sabotage the publish stage: bump the committed version of the record's page behind the transaction's back, so
      // commit2ndPhase fails with a ConcurrentModificationException INSIDE updatePages - after the (skipped) WAL stage.
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
      final PageId pageId = new PageId(database, rid.getBucketId(), (int) (rid.getPosition() / bucket.getMaxRecordsInPage()));
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final MutablePage conflicting = pageManager.getMutablePage(pageId, bucket.getPageSize(), false, false);
      pageManager.updatePages(null, Map.of(pageId, conflicting), false);

      final Throwable error = catchThrowable(() -> tx.commit2ndPhase(phase1));
      assertThat(error).isInstanceOf(ConcurrentModificationException.class);
      assertThat(database.isTransactionActive()).isFalse();

      // nothing was durable: the failed transaction must be rolled back, not just reset
      assertThat(created.getIdentity()).as("identity must be reset: no WAL record exists to ever replay this tx").isNull();
      assertThat(modified.getString("name")).as("modified record must be reloaded to the committed version").isEqualTo("committed");
    }
  }

  /**
   * Issue #4562 follow-up: a record created inside a transaction that is later rolled back (explicitly or by a failed
   * commit) must have its optimistically-assigned identity reset to provisional, so re-saving the same in-memory object
   * cleanly INSERTs a new record instead of being treated as an update of a now-missing record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RollbackResetIdentity extends TestHelper {

    private static final String TYPE = "SimpleVertexEx";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType(TYPE);
        type.createProperty("svex", String.class);
        type.createProperty("svuuid", String.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "svuuid");
      });
    }

    @Test
    void explicitRollbackResetsCreatedRecordIdentity() {
      database.begin();
      final MutableVertex v = database.newVertex(TYPE);
      v.set("svex", "a");
      v.set("svuuid", "u1");
      v.save();
      assertThat(v.getIdentity()).as("record gets an optimistic RID at creation").isNotNull();
      database.rollback();

      assertThat(v.getIdentity()).as("identity must be reset to provisional after rollback").isNull();

      // re-saving the same in-memory object must cleanly INSERT a new record
      database.begin();
      v.save();
      final RID newRid = v.getIdentity();
      database.commit();

      assertThat(newRid).as("a fresh RID is assigned on re-insert").isNotNull();
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    @Test
    void failedCommitResetsIdentityOfRecordsCreatedInThatTx() {
      // seed: v1 owns uuid u1
      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex(TYPE);
        v1.set("svex", "v1");
        v1.set("svuuid", "u1");
        v1.save();
      });

      // tx that fails on commit: v2 duplicates u1, v3 is a fresh record
      database.begin();
      final MutableVertex v2 = database.newVertex(TYPE);
      v2.set("svex", "v2");
      v2.set("svuuid", "u1"); // duplicate -> commit will fail
      v2.save();

      final MutableVertex v3 = database.newVertex(TYPE);
      v3.set("svex", "v3");
      v3.set("svuuid", "u3");
      v3.save();
      assertThat(v3.getIdentity()).isNotNull();

      final Throwable error = catchThrowable(() -> database.commit());
      assertThat(error).isInstanceOf(DuplicatedKeyException.class);
      assertThat(database.isTransactionActive()).isFalse();

      // both records created in the rolled-back tx must be provisional again
      assertThat(v2.getIdentity()).as("v2 identity reset after rollback").isNull();
      assertThat(v3.getIdentity()).as("v3 identity reset after rollback").isNull();

      // re-inserting v3 (fixing nothing) must work cleanly
      database.begin();
      v3.save();
      database.commit();

      assertThat(database.countType(TYPE, false)).as("v1 + re-inserted v3").isEqualTo(2);
    }

    @Test
    void successfulCommitDoesNotResetIdentity() {
      database.begin();
      final MutableVertex v = database.newVertex(TYPE);
      v.set("svex", "a");
      v.set("svuuid", "u1");
      v.save();
      database.commit();

      assertThat(v.getIdentity()).as("identity must survive a successful commit").isNotNull();
    }

    @Test
    void committedRecordKeepsIdentityEvenIfLaterDeleted() {
      // The new rollback reset must only affect records created in a transaction that ROLLED BACK. A record committed in
      // an earlier transaction must keep its identity, even if a later transaction (that also commits) deletes it.
      final MutableVertex[] vHolder = new MutableVertex[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex(TYPE);
        v.set("svex", "ghost");
        v.set("svuuid", "g1");
        v.save();
        vHolder[0] = v;
      });

      final RID committedRid = vHolder[0].getIdentity();
      assertThat(committedRid).isNotNull();

      database.transaction(() -> database.deleteRecord(database.lookupByRID(committedRid, false)));

      assertThat(vHolder[0].getIdentity()).as("a committed record's identity is never reset by a later tx").isEqualTo(committedRid);
    }
  }

  /**
   * Issue #10135: a {@code transaction()} call that JOINED the caller's transaction owns nothing, so a failure of its
   * block must leave the caller's transaction exactly as it found it - for EVERY exit of the loop, not only for the
   * retryable one. Before the fix the general {@code catch (Throwable)} arm rolled the caller's transaction back on any
   * non-retryable exception, so the caller's earlier work was discarded and its next write failed with
   * "Transaction not begun".
   */
  @Nested
  class JoinedTransactionFailureKeepsCallerTx extends TestHelper {
    private static final String TYPE = "Issue10135Doc";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE).createProperty("k", String.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "k");
      });
    }

    static Stream<Arguments> failures() {
      return Stream.of(//
          Arguments.of("retryable", true, (Supplier<Throwable>) () -> new ConcurrentModificationException("simulated conflict")),
          Arguments.of("duplicated key", true, (Supplier<Throwable>) () -> new DuplicatedKeyException("idx", "k", null)),
          Arguments.of("plain RuntimeException", false, (Supplier<Throwable>) () -> new IllegalStateException("boom")),
          Arguments.of("validation", false, (Supplier<Throwable>) () -> new ValidationException("invalid")),
          Arguments.of("Error", false, (Supplier<Throwable>) () -> new AssertionError("fatal")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failures")
    void aJoinedBlockThatThrowsLeavesTheCallersTransactionIntact(final String label, final boolean retryable,
        final Supplier<Throwable> failure) {
      database.begin();
      try {
        database.newDocument(TYPE).set("k", "outer").save();
        final int nestedBefore = database.getNestedTransactions();

        final Throwable expected = failure.get();
        final AtomicReference<Throwable> reported = new AtomicReference<>();

        assertThatThrownBy(() -> database.transaction(() -> {
          database.newDocument(TYPE).set("k", "inner").save();
          throwUnchecked(expected);
        }, true, 3, null, reported::set)).isSameAs(expected);

        assertThat(database.isTransactionActive()).as("the caller's transaction must survive a %s", label).isTrue();
        assertThat(database.getNestedTransactions()).isEqualTo(nestedBefore);
        if (retryable)
          assertThat(reported.get()).as("the joined retryable arm hands the failure to the owner without the callback")
              .isNull();
        else
          assertThat(reported.get()).as("the error callback must still be told on the non-retryable arm")
              .isSameAs(expected);

        // THE CALLER CAN KEEP WRITING AND COMMIT ITS OWN WORK
        database.newDocument(TYPE).set("k", "outer2").save();
        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(keys()).contains("outer", "outer2");
    }

    @Test
    void theDefaultOverloadJoinsAndLeavesTheCallersTransactionIntact() {
      // THE EXACT REPRO FROM THE ISSUE: db.transaction(block) NESTED IN db.transaction(block)
      database.transaction(() -> {
        database.newDocument(TYPE).set("k", "outer").save();
        try {
          database.transaction(() -> {
            throw new IllegalStateException("boom");
          });
        } catch (final IllegalStateException ignore) {
          // EXPECTED
        }
        assertThat(database.isTransactionActive()).isTrue();
        database.newDocument(TYPE).set("k", "outer2").save();
      });

      assertThat(keys()).containsExactlyInAnyOrder("outer", "outer2");
    }

    @Test
    void aSubTransactionTheBlockOpenedItselfIsRolledBackButTheCallersIsNot() {
      database.begin();
      try {
        database.newDocument(TYPE).set("k", "outer").save();
        final int nestedBefore = database.getNestedTransactions();

        assertThatThrownBy(() -> database.transaction(() -> {
          database.begin(); // A LEVEL THE BLOCK OWNS AND LEAVES BEHIND
          database.newDocument(TYPE).set("k", "leaked").save();
          throw new IllegalStateException("boom");
        }, true)).isInstanceOf(IllegalStateException.class);

        assertThat(database.isTransactionActive()).isTrue();
        assertThat(database.getNestedTransactions()).as("the level the block opened must be rolled back")
            .isEqualTo(nestedBefore);

        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(keys()).containsExactly("outer");
    }

    @Test
    void aSubTransactionCreatedWithoutJoiningIsRolledBackAndTheCallersSurvives() {
      database.begin();
      try {
        database.newDocument(TYPE).set("k", "outer").save();
        final int nestedBefore = database.getNestedTransactions();

        assertThatThrownBy(() -> database.transaction(() -> {
          database.newDocument(TYPE).set("k", "inner").save();
          throw new IllegalStateException("boom");
        }, false)).isInstanceOf(IllegalStateException.class);

        assertThat(database.isTransactionActive()).isTrue();
        assertThat(database.getNestedTransactions()).isEqualTo(nestedBefore);

        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(keys()).containsExactly("outer");
    }

    @Test
    void anOwnedTransactionIsFullyRolledBackEvenIfTheBlockLeftASubTransactionOpen() {
      assertThat(database.isTransactionActive()).isFalse();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("k", "owned").save();
        database.begin();
        database.newDocument(TYPE).set("k", "leaked").save();
        throw new IllegalStateException("boom");
      })).isInstanceOf(IllegalStateException.class);

      assertThat(database.isTransactionActive()).as("the transaction this call opened must not outlive it").isFalse();
      assertThat(keys()).isEmpty();
    }

    @Test
    void aRetriedAttemptStartsFromTheCallersDepthEvenIfTheFailedOneLeftASubTransactionOpen() {
      final int[] attempts = { 0 };

      database.transaction(() -> {
        database.newDocument(TYPE).set("k", "owned" + attempts[0]).save();
        if (attempts[0]++ == 0) {
          database.begin();
          throw new ConcurrentModificationException("simulated conflict");
        }
        assertThat(database.getNestedTransactions()).as("the retry must not run inside the leaked level").isEqualTo(1);
      }, false, 3);

      assertThat(attempts[0]).isEqualTo(2);
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(keys()).containsExactly("owned1");
    }

    private List<String> keys() {
      return database.query("sql", "select k from " + TYPE).stream().map(r -> r.<String>getProperty("k")).toList();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(final Throwable t) throws T {
      throw (T) t;
    }
  }

  /**
   * Issue #10386: an isolation level the engine cannot provide is refused on every surface, because silently giving a weaker
   * guarantee than the one asked for is the one failure a client cannot detect. {@link IsolationLevels} is the one parser the
   * surfaces share, and the SQL and openCypher entry points of this module go through it: asking for {@code SERIALIZABLE}
   * is refused with the same words, naming the levels the engine provides, instead of the JVM's own "No enum constant"
   * (SQL) or a message of that surface's own (openCypher).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnsupportedIsolationRefused extends TestHelper {

    @Test
    void theLevelsTheEngineProvidesAreParsedByTheirExactName() {
      assertThat(IsolationLevels.parse("READ_COMMITTED")).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      assertThat(IsolationLevels.parse("REPEATABLE_READ")).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    }

    @Test
    void aLevelTheEngineDoesNotProvideIsRefusedNamingTheOnesItDoes() {
      assertThatThrownBy(() -> IsolationLevels.parse("SERIALIZABLE")).isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid isolationLevel 'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]");
      assertThatThrownBy(() -> IsolationLevels.parse("READ_UNCOMMITTED")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> IsolationLevels.parse("repeatable_read")).as("the spelling is exact: a caller upper-cases first")
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> IsolationLevels.parse("")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> IsolationLevels.parse(null)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("accepted values are");
    }

    @Test
    void aSqlBeginOfAnUnsupportedLevelIsRefusedWithTheSharedMessage() {
      assertThatThrownBy(() -> database.command("sql", "BEGIN ISOLATION SERIALIZABLE")).hasStackTraceContaining(
          "Invalid isolationLevel 'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]");
      assertThat(database.isTransactionActive()).as("the refused BEGIN opened nothing").isFalse();
    }

    @Test
    void aSqlBeginOfASupportedLevelStillBegins() {
      database.command("sql", "BEGIN ISOLATION REPEATABLE_READ").close();
      try {
        assertThat(database.isTransactionActive()).isTrue();
        assertThat(((DatabaseInternal) database).getTransaction().getIsolationLevel())
            .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      } finally {
        database.rollback();
      }
    }

    @Test
    void aCypherStartTransactionOfAnUnsupportedLevelIsRefusedWithTheSharedMessage() {
      assertThatThrownBy(() -> database.command("opencypher", "START TRANSACTION ISOLATION SERIALIZABLE"))
          .hasStackTraceContaining("Invalid isolationLevel 'SERIALIZABLE': accepted values are [READ_COMMITTED, REPEATABLE_READ]");
      assertThat(database.isTransactionActive()).as("the refused START TRANSACTION opened nothing").isFalse();
    }

    @Test
    void aCypherStartTransactionOfASupportedLevelStillBegins() {
      database.command("opencypher", "START TRANSACTION ISOLATION REPEATABLE_READ").close();
      try {
        assertThat(database.isTransactionActive()).isTrue();
        assertThat(((DatabaseInternal) database).getTransaction().getIsolationLevel())
            .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      } finally {
        database.rollback();
      }
    }

    @Test
    void theCypherRefusalIsAParsingError() {
      assertThatThrownBy(() -> database.command("opencypher", "START TRANSACTION ISOLATION SERIALIZABLE"))
          .isInstanceOf(CommandParsingException.class);
    }
  }

  /**
   * Issue #10712, the counter half: at {@code REPEATABLE_READ} a scan repeats (its pages are pinned) but {@code count(*)} and
   * {@code countType()} answered the bucket's live counter plus the transaction's own delta, so after another thread's commit
   * they jumped to the new total while a scan of the same type, in the same transaction, still answered the old one. The
   * committed part of the counter is now pinned at the first count of the bucket, like a page at its first read.
   * <p>
   * The scan half of the issue - a scan reading its pages one after the other can see part of a concurrent multi-page commit
   * at both levels, because a page is pinned at its first read and not when the transaction begins - is the documented
   * contract ({@code Database.TRANSACTION_ISOLATION_LEVEL}), and {@link #lockTypeExcludesTheWritersOfAWholeTypeRead} pins the
   * way the documentation gives to read one committed state of a whole type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RepeatableReadCount extends TestHelper {
    private static final String TYPE = "T";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
      database.command("sql", "CREATE PROPERTY " + TYPE + ".id INTEGER");
      database.command("sql", "CREATE INDEX ON " + TYPE + " (id) UNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.command("sql", "INSERT INTO " + TYPE + " SET id = ?, v = 0", i);
      });
    }

    @Test
    void aCountRepeatsAtRepeatableReadAndAgreesWithTheScan() throws Exception {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(scan()).isEqualTo(100);
        assertThat(countStar()).isEqualTo(100);
        assertThat(database.countType(TYPE, true)).isEqualTo(100);

        foreignCommit();

        // THE REPORTER'S TABLE: SCAN, count(id) AND THE FILTERED COUNT STAYED AT 100, count(*) AND countType JUMPED TO 149
        assertThat(scan()).isEqualTo(100);
        assertThat(countStar()).as("count(*) repeats like the scan").isEqualTo(100);
        assertThat(database.countType(TYPE, true)).as("countType repeats like the scan").isEqualTo(100);
        assertThat(count("SELECT count(id) AS c FROM " + TYPE)).isEqualTo(100);

        // THE TRANSACTION STILL COUNTS ITS OWN WRITES ON TOP OF WHAT IT PINNED
        database.command("sql", "INSERT INTO " + TYPE + " SET id = 1000, v = 0");
        assertThat(countStar()).isEqualTo(101);
        assertThat(database.countType(TYPE, true)).isEqualTo(101);
      } finally {
        database.rollback();
      }

      // THE PIN DIES WITH THE TRANSACTION: OUTSIDE IT, AND IN THE NEXT ONE, THE COUNT IS THE COMMITTED ONE
      assertThat(countStar()).isEqualTo(149);
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(countStar()).isEqualTo(149);
        assertThat(scan()).isEqualTo(149);
      } finally {
        database.rollback();
      }
    }

    /**
     * The other order, which the javadoc names: a count pins the counter at once and a scan pins its pages only when it reads
     * them, so a commit landing between the two makes them disagree inside one REPEATABLE_READ transaction. Each of the two
     * repeats; neither is a snapshot (#10792).
     */
    @Test
    void aCountBeforeTheFirstScanPinsEarlierThanTheScan() throws Exception {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(countStar()).isEqualTo(100);
        foreignCommit();
        assertThat(scan()).as("the pages are first read after the commit").isEqualTo(149);
        assertThat(countStar()).as("the counter was pinned before it").isEqualTo(100);
        assertThat(scan()).as("the scan repeats").isEqualTo(149);
      } finally {
        database.rollback();
      }
    }

    /**
     * The context outlives its transaction and keeps its REPEATABLE_READ level: a count between two transactions must not pin
     * a value the next transaction would start from.
     */
    @Test
    void aCountBetweenTwoTransactionsPinsNothing() throws Exception {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      assertThat(countStar()).isEqualTo(100);
      database.rollback();

      // OUTSIDE A TRANSACTION, ON THE SAME (NOW INACTIVE) CONTEXT
      assertThat(database.countType(TYPE, true)).isEqualTo(100);
      foreignCommit();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(countStar()).as("the new transaction counts from the committed state it starts in").isEqualTo(149);
      } finally {
        database.rollback();
      }
    }

    @Test
    void aCountFollowsTheCommitsAtReadCommitted() throws Exception {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        assertThat(countStar()).isEqualTo(100);
        foreignCommit();
        assertThat(countStar()).isEqualTo(149);
        assertThat(database.countType(TYPE, true)).isEqualTo(149);
        assertThat(scan()).isEqualTo(149);
      } finally {
        database.rollback();
      }
    }

    /**
     * What the documentation offers a reader that needs one committed state of a whole type: an explicit lock on the type
     * keeps every writer of it out until the reading transaction ends, so no commit can land between two of its page reads.
     */
    @Test
    void lockTypeExcludesTheWritersOfAWholeTypeRead() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);
      try {
        database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        try {
          database.command("sql", "LOCK TYPE " + TYPE);
          assertThat(scan()).isEqualTo(100);

          final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
          final Thread writer = new Thread(() -> {
            try {
              database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET id = 500, v = 0"), false, 1);
            } catch (final Throwable e) {
              writerFailure.set(e);
            }
          });
          writer.start();
          writer.join(30_000);

          assertThat(writerFailure.get()).as("a writer of the locked type cannot commit while the read runs")
              .isInstanceOf(LockTimeoutException.class);
          assertThat(scan()).isEqualTo(100);
        } finally {
          database.rollback();
        }
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getDefValue());
      }
      assertThat(countStar()).isEqualTo(100);
    }

    /** The reporter's foreign commit, from another thread: 50 inserts, one update and one delete, 100 rows become 149. */
    private void foreignCommit() throws InterruptedException {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.transaction(() -> {
            for (int i = 100; i < 150; i++)
              database.command("sql", "INSERT INTO " + TYPE + " SET id = ?, v = 0", i);
            database.command("sql", "UPDATE " + TYPE + " SET v = 1 WHERE id = 5");
            database.command("sql", "DELETE FROM " + TYPE + " WHERE id = 6");
          });
        } catch (final Throwable e) {
          failure.set(e);
        }
      });
      writer.start();
      writer.join(30_000);
      assertThat(failure.get()).isNull();
    }

    private long scan() {
      long n = 0;
      try (final ResultSet rs = database.query("sql", "SELECT id FROM " + TYPE)) {
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
      }
      return n;
    }

    private long countStar() {
      return count("SELECT count(*) AS c FROM " + TYPE);
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  /**
   * Issue #10768: with {@code arcadedb.txWAL=false} no WAL record is appended, so a commit whose page publication failed halfway was
   * concluded with a full {@code rollback()}: the caller was told nothing happened while the pages already written stayed on disk
   * and in the read cache, and the records created in the transaction had their identities reset, so a retry inserted them again.
   * The start of the publication is now a point of no return of its own: past it the database is fenced and the transaction is
   * released without touching the records the caller holds, as a failure past the WAL append is.
   * <p>
   * The page write that fails is chosen in turn, from the first page of the transaction to the last but one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NoWalPartialPublish extends TestHelper {
    private static final int RECORDS = 3_000;

    @Test
    void aPublicationThatFailsOnAnyPageFencesAndKeepsTheRecordsTheCallerHolds() {
      final int pages = pagesWrittenByOneTransaction();
      assertThat(pages).as("the transaction must span several pages for a failure to land halfway").isGreaterThan(2);

      for (int failingWrite = 1; failingWrite < pages; failingWrite++) {
        final String typeName = "Doc" + failingWrite;
        database.getSchema().createDocumentType(typeName);
        noWalSynchronousFlush();

        final List<MutableDocument> created = new ArrayList<>(RECORDS);
        final AtomicInteger writes = new AtomicInteger();
        final int failAt = failingWrite;
        try {
          assertThatThrownBy(() -> database.transaction(() -> {
            for (int i = 0; i < RECORDS; i++)
              created.add(database.newDocument(typeName).set("i", i).set("payload", "x".repeat(64)).save());
            // ARMED LAST, SO ONLY THE PAGES OF THIS COMMIT COUNT
            PageManager.setPageWriteFaultInjector(pageId -> {
              if (writes.incrementAndGet() == failAt)
                throw new IOException("injected failure of page write " + failAt);
            });
          }, false, 0)).isInstanceOf(TransactionException.class);
        } finally {
          PageManager.setPageWriteFaultInjector(null);
        }

        assertThat(writes.get()).as("the injected failure was reached").isGreaterThanOrEqualTo(failAt);
        // NOT "NOTHING HAPPENED": THE DATABASE REFUSES TO COMMIT ON TOP OF A HALF PUBLISHED TRANSACTION...
        assertThat(isFenced()).as("fenced, failing write %d", failAt).isTrue();
        // ...AND THE RECORDS THE CALLER HOLDS KEEP THE IDENTITIES SOME OF THEM ALREADY HAVE ON DISK, SO A RETRY CANNOT DUPLICATE THEM
        assertThat(created).hasSize(RECORDS).allSatisfy(doc -> assertThat(doc.getIdentity()).isNotNull());

        reopenDatabase();
      }
    }

    @Test
    void aFailureBeforeThePublicationStartsStillRollsBack() {
      database.getSchema().createDocumentType("Doc");
      noWalSynchronousFlush();

      final List<MutableDocument> created = new ArrayList<>();
      assertThatThrownBy(() -> database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          created.add(database.newDocument("Doc").set("i", i).save());
        // REFUSED BY THE 1ST PHASE: NOTHING WAS PUBLISHED, SO THE ROLLBACK IS THE RIGHT CONCLUSION AND NOTHING IS FENCED
        ((DatabaseInternal) database).getTransaction().setRollbackOnly("refused before the publication");
      }, false, 0)).isInstanceOf(TransactionException.class);

      assertThat(isFenced()).isFalse();
      assertThat(created).allSatisfy(doc -> assertThat(doc.getIdentity()).isNull());
      assertThat(database.countType("Doc", false)).isZero();
    }

    /** The pages a commit of {@link #RECORDS} records writes, measured on a type of its own with the same flush settings. */
    private int pagesWrittenByOneTransaction() {
      database.getSchema().createDocumentType("Probe");
      noWalSynchronousFlush();
      final AtomicInteger writes = new AtomicInteger();
      try {
        database.transaction(() -> {
          for (int i = 0; i < RECORDS; i++)
            database.newDocument("Probe").set("i", i).set("payload", "x".repeat(64)).save();
          PageManager.setPageWriteFaultInjector(pageId -> writes.incrementAndGet());
        });
      } finally {
        PageManager.setPageWriteFaultInjector(null);
      }
      return writes.get();
    }

    private boolean isFenced() {
      return ((DatabaseInternal) database).getEmbedded() instanceof LocalDatabase local && local.isFencedForRecovery();
    }

    private void noWalSynchronousFlush() {
      database.setUseWAL(false);
      database.setAsyncFlush(false);
    }
  }

  /**
   * Issue #10881: {@link LocalDatabase#awaitCommitsInFlight()} waited only for the holders of the database lock, and an HA leader
   * commit goes on after it leaves the lock - quorum, publication, acknowledgement - so a restore that refused writes and awaited
   * the commits in flight swapped its target away under a commit the client was then told had succeeded. A commit that goes on
   * outside the lock now registers itself under it, and the await waits for it to end.
   * <p>
   * The HA commit path itself is driven by {@code HaRestoreClaimIT} in ha-raft. This drives the engine
   * half: the counter, the await, and the schema change that waited for the write lock while the refusal went up.
   */
  @Nested
  class CommitsInFlightOutsideLock extends TestHelper {

    private static final String REASON = "issue 10881";

    @Override
    protected void endTest() {
      local().acceptWrites(REASON);
    }

    private LocalDatabase local() {
      return (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    }

    @Test
    void theAwaitWaitsForACommitRegisteredAsInFlightOutsideTheLock() throws Exception {
      final LocalDatabase local = local();
      // What an HA leader's phase 1 does: registered under the shared lock, after the refusal check, then the lock is released
      local.executeInReadLock(() -> {
        local.beginCommitInFlight();
        return null;
      });
      assertThat(local.getCommitsInFlight()).isEqualTo(1);

      final ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        local.refuseWrites(REASON, true);
        final Future<?> await = executor.submit(local::awaitCommitsInFlight);
        // Expected to time out: the commit is still on its way to quorum
        assertThatThrownBy(() -> await.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

        local.endCommitInFlight();
        await.get(30, TimeUnit.SECONDS);
        assertThat(local.getCommitsInFlight()).isZero();
      } finally {
        executor.shutdownNow();
      }
    }

    @Test
    void theAwaitReturnsAtOnceWithNothingInFlight() {
      local().refuseWrites(REASON, true);
      local().awaitCommitsInFlight();
      assertThat(local().getCommitsInFlight()).isZero();
    }

    @Test
    void anInterruptedAwaitGivesUpAndKeepsTheInterruptFlag() throws Exception {
      final LocalDatabase local = local();
      local.executeInReadLock(() -> {
        local.beginCommitInFlight();
        return null;
      });
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean flagKept = new AtomicBoolean();
      final Thread waiter = new Thread(() -> {
        try {
          local.awaitCommitsInFlight();
        } catch (final Throwable t) {
          failure.set(t);
          flagKept.set(Thread.currentThread().isInterrupted());
        }
      });
      try {
        waiter.start();
        Awaitility.await().atMost(30, TimeUnit.SECONDS)
            .until(() -> waiter.getState() == Thread.State.TIMED_WAITING || waiter.getState() == Thread.State.WAITING);
        waiter.interrupt();
        waiter.join(30_000);
        assertThat(failure.get()).isInstanceOf(DatabaseOperationException.class).hasMessageContaining("in flight");
        assertThat(flagKept.get()).isTrue();
      } finally {
        local.endCommitInFlight();
      }
    }

    /**
     * A schema change checked the refusal before it waited for the write lock, and nowhere after. A refusal raised meanwhile was
     * followed by an await that took the lock first and returned, and the schema change then ran unrefused under a restore that
     * had already stopped waiting for writers.
     */
    @Test
    void aSchemaChangeThatWaitedForTheLockWhileTheRefusalWentUpIsRefused() throws Exception {
      final LocalDatabase local = local();
      final CountDownLatch holding = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final AtomicBoolean callbackRan = new AtomicBoolean();
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        final Future<?> holder = executor.submit(() -> local.executeInWriteLock(() -> {
          holding.countDown();
          release.await();
          return null;
        }));
        assertThat(holding.await(30, TimeUnit.SECONDS)).isTrue();

        final AtomicReference<Thread> schemaThread = new AtomicReference<>();
        final Future<?> schemaChange = executor.submit(() -> {
          schemaThread.set(Thread.currentThread());
          return local.recordFileChanges(() -> {
            callbackRan.set(true);
            return null;
          });
        });
        // Parked on the write lock: past the check made before it
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
          final Thread t = schemaThread.get();
          return t != null && (t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING);
        });

        local.refuseWrites(REASON, true);
        release.countDown();
        holder.get(30, TimeUnit.SECONDS);

        assertThatThrownBy(() -> schemaChange.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(DatabaseNotAvailableException.class);
        assertThat(callbackRan.get()).as("the schema change must not run under the refusal").isFalse();
      } finally {
        release.countDown();
        executor.shutdownNow();
      }
    }
  }

  /**
   * Issue #10012: a schema change takes the database WRITE lock, and a thread that already holds the READ lock - code
   * running inside {@code lookupByRID}, such as a record read listener - can never be granted it, because a
   * {@code ReentrantReadWriteLock} does not upgrade. The thread parked on itself forever. It is now refused with an
   * exception that says why, and the database stays usable.
   * <p>
   * Every test carries a {@link Timeout}: before the fix the first one does not fail, it hangs.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReadLockUpgrade extends TestHelper {

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES) // HANG DETECTOR, NOT A LATENCY BOUND
    void aSchemaChangeFromAReadListenerIsRefusedInsteadOfDeadlocking() {
      final DocumentType type = database.getSchema().createDocumentType("Doc");
      type.createProperty("name", Type.STRING);

      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("name", "a").save().getIdentity());

      final AfterRecordReadListener ddlOnRead = record -> {
        database.getSchema().getType("Doc").getProperty("name").setMandatory(true);
        return record;
      };
      type.getEvents().registerListener(ddlOnRead);
      try {
        assertThatThrownBy(() -> database.lookupByRID(rid[0], true))
            .isInstanceOf(DatabaseOperationException.class)
            .hasMessageContaining("a read lock cannot be upgraded");
      } finally {
        type.getEvents().unregisterListener(ddlOnRead);
      }

      // NOTHING WAS LEFT HALF-TAKEN: THE SAME SCHEMA CHANGE WORKS OUTSIDE THE LISTENER, AND THE RECORD READS
      assertThat(database.getSchema().getType("Doc").getProperty("name").isMandatory()).isFalse();
      database.getSchema().getType("Doc").getProperty("name").setMandatory(true);
      assertThat(database.getSchema().getType("Doc").getProperty("name").isMandatory()).isTrue();
      assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo("a");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void aReaderAskingForTheWriteLockIsRefused() {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();

      assertThatThrownBy(() -> local.executeInReadLock(() -> local.executeInWriteLock(() -> null)))
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("a read lock cannot be upgraded");

      // AND THE LOCK IS STILL FREE FOR A WRITER AFTERWARDS
      assertThat(local.executeInWriteLock(() -> "written")).isEqualTo("written");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void aWriterThatAlsoTookTheReadLockStillReentersTheWriteLock() {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();

      // A DOWNGRADE IS LEGAL: THE THREAD OWNS THE WRITE LOCK, SO ASKING FOR IT AGAIN IS A RE-ENTRY, NOT AN UPGRADE
      final Object result = local.executeInWriteLock(
          () -> local.executeInReadLock(() -> local.executeInWriteLock(() -> "nested")));

      assertThat(result).isEqualTo("nested");
    }
  }

  /**
   * Issue #10379: the writes a statement gives back when it takes back a record it created have a floor, what the transaction had
   * counted when the statement started, so a wrong count of the caller can never lower the safety counter under it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RetractionFloor extends TestHelper {

    @Test
    void aRetractionNeverTakesTheCounterBelowWhereTheStatementStarted() {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.v INTEGER");

      database.begin();
      try {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        database.command("sql", "INSERT INTO T SET v = 1").close();
        final long afterTheCallersWrite = tx.getCompletedRecordWrites();
        assertThat(afterTheCallersWrite).isPositive();

        // a statement that started here and counted two writes can give two back, and never more than it counted
        tx.countCompletedRecordWrite();
        tx.countCompletedRecordWrite();
        tx.retractCompletedRecordWrites(2L, afterTheCallersWrite);
        assertThat(tx.getCompletedRecordWrites()).isEqualTo(afterTheCallersWrite);
        tx.retractCompletedRecordWrites(100L, afterTheCallersWrite);
        assertThat(tx.getCompletedRecordWrites()).isEqualTo(afterTheCallersWrite);
      } finally {
        database.rollback();
      }
    }
  }

  /**
   * Issue #5608 (follow-up 4 of #5596): {@code TransactionContext.reportCoverageDecline} is a pure diagnostic, and it
   * runs from INSIDE the commit loop's {@code catch (ConcurrentModificationException e)}, one statement before
   * {@code throw e}. Anything it throws therefore replaces the conflict the caller is propagating.
   * <p>
   * That was not merely a misleading message. The diagnostic reaches {@code edgeSegmentPageKey}, and its bucket lookup
   * used to raise {@code SchemaException} for an unknown id - the {@code bucket == null} branch below it never fired,
   * because the single-argument {@code LocalSchema.getBucketById(id)} throws before it can return null. A
   * {@code SchemaException} is NOT a {@code NeedRetryException}, so the commit's generic {@code catch (Exception)} would
   * have wrapped it into a plain {@code TransactionException}, turning a conflict the caller would have retried into a
   * hard failure. Both halves are fixed: the lookup now asks for the null its own contract was written against, so a
   * bucket it cannot resolve is the retryable conflict it always claimed to be.
   * <p>
   * The diagnostic is total by construction on top of that, which is what this test pins: the setup is verified to be
   * one where resolving the segment's page key really does raise, and the report still returns quietly.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CoverageDeclineDiagnostic extends TestHelper {

    @Test
    void aFailingDiagnosticNeverReplacesTheConflictBeingReported() throws Exception {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Diag", 1);
        database.newDocument("Diag").set("tag", "value").save();
      });

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(
          database.getSchema().getType("Diag").getBuckets(false).getFirst().getFileId());

      database.begin();
      try {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

        // A tracked edge append whose segment claims a bucket id no bucket answers to. trackEdgeAppend itself never
        // resolves the page key (that is what keeps the append hot path allocation-free), so the transaction is left
        // holding a tracked segment nobody can locate - the state the diagnostic then walks into.
        final int unmappedBucketId = firstUnmappedBucketId();
        final RID unresolvableSegment = new RID(unmappedBucketId, 0);
        tx.trackEdgeAppend(unresolvableSegment, new RID(unmappedBucketId, 1), new RID(unmappedBucketId, 2));

        // The setup is genuinely hostile: pin it, so the test cannot quietly degrade into asserting that a no-op does
        // not throw. And pin the KIND - a bucket the edge merge cannot resolve is a retryable conflict by contract, so
        // a regression back to the bare SchemaException (non-retryable, hard-failing the commit) fails here too.
        assertThatThrownBy(() -> tx.poisonEdgeAppendPage(unresolvableSegment))
            .as("resolving the segment's page key must really raise").isInstanceOf(ConcurrentModificationException.class)
            .isInstanceOf(NeedRetryException.class);

        final MutablePage page = tx.getPageToModify(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize(), false);

        final Method reportCoverageDecline = TransactionContext.class.getDeclaredMethod("reportCoverageDecline",
            MutablePage.class);
        reportCoverageDecline.setAccessible(true);

        assertThatCode(() -> reportCoverageDecline.invoke(tx, page))
            .as("the diagnostic must never alter the exception the commit loop is propagating").doesNotThrowAnyException();
      } finally {
        database.rollback();
      }

      // The database is untouched by the probe above.
      database.transaction(() -> assertThat(database.countType("Diag", false)).isEqualTo(1));
    }

    /**
     * The lowest file id the single-argument {@code getBucketById} refuses - either past the end of the file list, or a
     * component that is not a bucket (the schema dictionary, an index). Derived rather than hard-coded to a large
     * constant, so the fixture cannot quietly grow into the id the test assumed was free.
     */
    private int firstUnmappedBucketId() {
      int id = 0;
      // The two-argument form returns null for exactly the ids the single-argument one raises on.
      while (database.getSchema().getEmbedded().getBucketById(id, false) != null)
        ++id;
      return id;
    }
  }

  /**
   * Regression for issue #7916: {@code db.transaction(txBlock, joinCurrentTx, attempts, ...)} rolls back and re-runs
   * the whole block on a {@code NeedRetryException}. A rollback can only take back what is still buffered, so a block
   * containing a statement that COMMITS MID-EXECUTION - {@code UPDATE/DELETE/MOVE VERTEX ... BATCH n}, or DDL such as
   * {@code TRUNCATE TYPE} and {@code REBUILD INDEX} - has its durable half applied a SECOND time by the retry, and
   * {@code ok.call()} then reports clean success. {@code BatchStep} re-begins straight after its commit, so on return
   * the database looks exactly as it did going in and the loop cannot tell from {@code isTransactionActive()} that
   * anything happened.
   * <p>
   * #7667 gave {@code TransactionContext} the commit counter that detects this and wired it into the async executor's
   * shared batch; the two block-level retry loops did not consult it. The fix makes them refuse the retry and
   * propagate the conflict to the caller, who is the only one who knows how to compensate for the half that stands.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RetryDoesNotReplayCommittedHalf extends TestHelper {

    private static final String ROWS = "Issue7916Row";
    private static final int    SIZE = 25;

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(ROWS).createProperty("seq", Type.INTEGER);

      database.transaction(() -> {
        for (int i = 0; i < SIZE; i++)
          database.newDocument(ROWS).set("seq", 0).save();
      });
    }

    /**
     * The defect itself: with a BATCH boundary crossed before the conflict, the retry used to double-apply the rows
     * already published. Now the block is not re-run at all, so every row is incremented exactly once, and the
     * conflict reaches the caller instead of being swallowed into a clean return.
     */
    @Test
    void aBlockThatCommittedPartOfItsWorkIsNotReplayed() {
      final AtomicInteger attempts = new AtomicInteger();
      final AtomicInteger okCalls = new AtomicInteger();

      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        // Commits every 10 of the 25 rows: 20 are durable by the time the statement ends.
        database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10");
        // ... and then the block fails, the way a genuine MVCC conflict on a later statement would.
        throw new ConcurrentModificationException("simulated conflict after the batch boundary");
      }, true, 3, okCalls::incrementAndGet, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
      assertThat(okCalls.get()).as("and a block that failed must not report success").isZero();

      // The 20 rows the batch published are incremented exactly once; the remaining 5 were rolled back.
      assertThat(countRowsWithSeq(1)).isEqualTo(20);
      assertThat(countRowsWithSeq(0)).isEqualTo(5);
      assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
    }

    /**
     * The issue also named {@code TRUNCATE TYPE} and {@code REBUILD TYPE} as mid-statement committers. They are NOT,
     * at this HEAD: both suppress their batching when a caller transaction is active (issue #6220), so they join the
     * caller's unit and a rollback really does put the records back. The retry therefore stays available, and this
     * pins that - a future change that made either of them commit under a caller transaction would trip the guard
     * here and be caught as the behaviour change it is.
     * <p>
     * {@code BATCH n} is deliberately the exception: the clause IS the user asking for intermediate commits.
     */
    @Test
    void truncateInsideACallerTransactionJoinsItSoTheRetryStaysAvailable() {
      final AtomicInteger attempts = new AtomicInteger();

      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        database.command("sql", "TRUNCATE TYPE " + ROWS);
        throw new ConcurrentModificationException("simulated conflict after the truncate");
      }, true, 3, null, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).as("nothing was published, so every attempt is still available").isEqualTo(3);
      assertThat(countRowsWithSeq(0)).as("and the rollback put every row back").isEqualTo(SIZE);
    }

    /**
     * {@code DELETE ... BATCH n} goes through the same {@code BatchStep}, so it gets the same refusal.
     */
    @Test
    void aBatchedDeleteThatCommittedPartOfItsWorkIsNotReplayed() {
      final AtomicInteger attempts = new AtomicInteger();

      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        database.command("sql", "DELETE FROM " + ROWS + " BATCH 10");
        throw new ConcurrentModificationException("simulated conflict after the batch boundary");
      }, true, 3, null, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).isEqualTo(1);
      // 20 rows are durably gone, the last 5 deletes were rolled back.
      assertThat(countRowsWithSeq(0)).isEqualTo(5);
    }

    /**
     * The guard must not disturb the ordinary case it sits next to: a block that publishes NOTHING before failing is
     * still retried the full number of attempts, because the rollback really did take everything back.
     */
    @Test
    void aBlockThatCommittedNothingIsStillRetried() {
      final AtomicInteger attempts = new AtomicInteger();

      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        database.newDocument(ROWS).set("seq", 99).save();
        throw new ConcurrentModificationException("simulated conflict with nothing published");
      }, true, 3, null, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).as("nothing was durable, so all the attempts are still available").isEqualTo(3);
      assertThat(countRowsWithSeq(99)).as("and the rollback took every attempt's work back").isZero();
    }

    /**
     * And a block that succeeds on a later attempt still succeeds: the guard reads the commit counter of the
     * transaction the attempt ran in, not of the whole database, so an earlier attempt's rollback cannot make a
     * later one look partially committed.
     */
    @Test
    void aBlockThatSucceedsOnRetryStillSucceeds() {
      final AtomicInteger attempts = new AtomicInteger();

      database.transaction(() -> {
        if (attempts.incrementAndGet() < 3)
          throw new ConcurrentModificationException("simulated transient conflict");
        database.newDocument(ROWS).set("seq", 42).save();
      }, true, 5, null, null);

      assertThat(attempts.get()).isEqualTo(3);
      assertThat(countRowsWithSeq(42)).isEqualTo(1);
    }

    /**
     * The second loop with the same hazard, and the one {@code commitBatch()}'s javadoc cites as the precedent for
     * the async retry contract: {@code DatabaseAsyncTransaction.executeTransaction}.
     */
    @Test
    void theAsyncRetryLoopRefusesToReplayACommittedHalf() throws Exception {
      final AtomicInteger      attempts = new AtomicInteger();
      final CountDownLatch     finished = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      database.async().transaction(() -> {
        attempts.incrementAndGet();
        database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10");
        throw new ConcurrentModificationException("simulated conflict after the batch boundary");
      }, 3, null, e -> {
        failure.set(e);
        finished.countDown();
      });

      assertThat(finished.await(60, TimeUnit.SECONDS)).as("the async task reported back").isTrue();
      database.async().waitCompletion();

      assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
      assertThat(failure.get()).isInstanceOf(NeedRetryException.class);

      assertThat(countRowsWithSeq(1)).isEqualTo(20);
      assertThat(countRowsWithSeq(0)).isEqualTo(5);
      assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
    }

    private long countRowsWithSeq(final int seq) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  /**
   * Regression test for issue #9995: the local retry loops ({@code Database.transaction}, {@code COMMIT RETRY}) retried every
   * {@link NeedRetryException}, including a refusal this node repeats until something outside it changes - a follower refusing
   * leader-only work and naming the leader - so the caller learnt where to go only after every attempt and every retry delay.
   * Such a refusal answers {@link NeedRetryException#isRetryableOnThisNode()} with {@code false} and is rethrown at once.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NodeBoundRefusalNotRetried extends TestHelper {
    private static final String FUNCTION = "issue9995_refuse";

    /** A refusal that holds on this node, as a follower's refusal of leader-only work naming the leader does. */
    private static class RefusalHoldingHere extends NeedRetryException {
      RefusalHoldingHere() {
        super("leader-only work refused on this node");
      }

      @Override
      public boolean isRetryableOnThisNode() {
        return false;
      }
    }

    @Test
    void transactionRethrowsANodeBoundRefusalWithoutRetrying() {
      final AtomicInteger attempts = new AtomicInteger();
      final AtomicInteger errorCallbacks = new AtomicInteger();
      final RefusalHoldingHere refusal = new RefusalHoldingHere();

      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        database.newDocument("Doc9995").set("a", 1).save();
        throw refusal;
      }, false, 5, null, e -> errorCallbacks.incrementAndGet())).isSameAs(refusal);

      assertThat(attempts.get()).isEqualTo(1);
      assertThat(errorCallbacks.get()).isEqualTo(1);
      assertThat(database.isTransactionActive()).as("the attempt is rolled back").isFalse();
      assertThat(database.countType("Doc9995", false)).isZero();
    }

    @Test
    void transactionStillRetriesAnOrdinaryRetryableFailure() {
      final AtomicInteger attempts = new AtomicInteger();

      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        throw new NeedRetryException("conflict");
      }, false, 3)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void commitRetryRethrowsANodeBoundRefusalWithoutRetrying() {
      final AtomicInteger calls = new AtomicInteger();
      final AtomicReference<NeedRetryException> toThrow = new AtomicReference<>();
      final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
      sql.getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
        @Override
        public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
            final CommandContext context) {
          calls.incrementAndGet();
          throw toThrow.get();
        }

        @Override
        public String getSyntax() {
          return FUNCTION + "()";
        }
      });
      try {
        // THE CONTROL FIRST: AN ORDINARY CONFLICT RAISED BY THE FUNCTION REACHES THE RETRY STEP AND IS RETRIED, SO THE ASSERTION
        // BELOW CANNOT PASS BECAUSE THE EXCEPTION NEVER GOT THERE
        toThrow.set(new NeedRetryException("conflict"));
        assertThatThrownBy(() -> database.command("sqlscript", "BEGIN; SELECT " + FUNCTION + "(); COMMIT RETRY 4;"))
            .isInstanceOf(NeedRetryException.class);
        assertThat(calls.get()).as("an ordinary conflict uses every attempt").isEqualTo(4);

        calls.set(0);
        toThrow.set(new RefusalHoldingHere());
        assertThatThrownBy(() -> database.command("sqlscript", "BEGIN; SELECT " + FUNCTION + "(); COMMIT RETRY 4;"))
            .isInstanceOf(RefusalHoldingHere.class);
        assertThat(calls.get()).as("a refusal that holds on this node is not retried").isEqualTo(1);
      } finally {
        sql.getFunctionFactory().unregister(FUNCTION);
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Doc9995");
    }
  }

  /**
   * Regression test for GitHub issue #7149.
   * <p>
   * Writing to a record that the SAME transaction has already deleted used to be queued for the deferred
   * commit-time write like any other update. {@code commit1stPhase()} then found the record missing and reported a
   * {@link ConcurrentModificationException} saying it "was deleted by a concurrent transaction" - naming a
   * concurrent transaction that never existed. Because that is a {@code NeedRetryException}, the caller re-ran the
   * whole command {@code arcadedb.txRetries} times against a state that could never change, and a single Bolt
   * client was finally told {@code Neo.TransientError.Transaction.DeadlockDetected}.
   * <p>
   * The delete wins instead: the record cannot exist at commit, so the write is dropped, exactly as the symmetric
   * order (an update queued BEFORE the delete, which {@code removeRecordFromCache} drops) already behaved. The
   * commit-time arm stays for what it was written for (#4959): a delete by a genuinely concurrent transaction.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UpdateAfterDeleteInSameTx {
    private static final String DB_PATH = "./target/databases/testissue7149-update-after-delete";

    private DatabaseInternal database;

    @BeforeEach
    void setUp() {
      database = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
      final VertexType type = database.getSchema().createVertexType("Doc");
      type.createProperty("id", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "id");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private RID createRecord(final int id) {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Doc");
        v.set("id", id);
        v.set("value", "initial");
        v.save();
        rid[0] = v.getIdentity();
      });
      return rid[0];
    }

    private long countRecords() {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Doc")) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private List<RID> indexEntriesFor(final int id) {
      final IndexCursor cursor = database.getSchema().getIndexByName("Doc[id]").get(new Object[] { id });
      final List<RID> found = new ArrayList<>();
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
      return found;
    }

    @Test
    void savingAStaleInstanceAfterAnInTransactionDeleteDoesNotReportAPhantomConflict() {
      final RID rid = createRecord(1);

      assertThatCode(() -> database.transaction(() -> {
        final MutableVertex stale = database.lookupByRID(rid, true).asVertex().modify();
        stale.set("value", "updated");
        stale.save();

        database.deleteRecord(database.lookupByRID(rid, true));
        assertThat(database.getTransaction().isDeletedInTransaction(rid)).isTrue();

        // The stale instance is still live in the caller's hands - this is what an OpenCypher row fanout, or any
        // application holding the record across the delete, does. Pre-fix this queued the write and the COMMIT blew
        // up with "deleted by a concurrent transaction".
        stale.set("value", "after-delete");
        stale.save();
      })).doesNotThrowAnyException();

      assertThat(countRecords()).as("the delete wins: the record must be gone").isZero();
    }

    @Test
    void theDroppedWriteLeavesNoPhantomIndexEntry() {
      final RID rid = createRecord(7);

      database.transaction(() -> {
        final MutableVertex stale = database.lookupByRID(rid, true).asVertex().modify();
        database.deleteRecord(database.lookupByRID(rid, true));
        // A write that changes the INDEXED property: the index maintenance must be skipped along with the write,
        // otherwise the delete's own index cleanup is undone and the key resurrects pointing at a dead RID.
        stale.set("id", 8);
        stale.save();
      });

      assertThat(countRecords()).isZero();
      assertThat(indexEntriesFor(7)).as("the delete removed the original key").isEmpty();
      assertThat(indexEntriesFor(8)).as("the dropped write must not have added a key").isEmpty();
    }

    @Test
    void aRecordDeletedInThisTransactionDoesNotBlockUpdatesToOtherRecords() {
      final RID deleted = createRecord(2);
      final RID survivor = createRecord(3);

      database.transaction(() -> {
        database.deleteRecord(database.lookupByRID(deleted, true));

        final MutableVertex v = database.lookupByRID(survivor, true).asVertex().modify();
        v.set("value", "still-written");
        v.save();
      });

      assertThat(countRecords()).isEqualTo(1);
      assertThat(database.lookupByRID(survivor, true).asVertex().getString("value")).isEqualTo("still-written");
    }

    @Test
    void aDroppedWriteIsNotCountedAsAnUpdate() {
      final RID rid = createRecord(5);

      final long before = ((Number) database.getStats().get("updateRecord")).longValue();
      database.transaction(() -> {
        final MutableVertex stale = database.lookupByRID(rid, true).asVertex().modify();
        database.deleteRecord(database.lookupByRID(rid, true));
        stale.set("value", "dropped");
        stale.save();
      });

      assertThat(((Number) database.getStats().get("updateRecord")).longValue())
          .as("a write that was never applied is not an update").isEqualTo(before);
    }

    @Test
    void anOrdinaryUpdateIsStillCounted() {
      final RID rid = createRecord(6);

      final long before = ((Number) database.getStats().get("updateRecord")).longValue();
      database.transaction(() -> {
        final MutableVertex v = database.lookupByRID(rid, true).asVertex().modify();
        v.set("value", "counted");
        v.save();
      });

      assertThat(((Number) database.getStats().get("updateRecord")).longValue()).isEqualTo(before + 1);
    }

    @Test
    void anUpdateQueuedBeforeTheDeleteIsStillDropped() {
      // The symmetric order, which removeRecordFromCache() has always handled: pinned here so the two halves of the
      // invariant the commit-time arm relies on stay together.
      final RID rid = createRecord(4);

      database.transaction(() -> {
        final MutableVertex v = database.lookupByRID(rid, true).asVertex().modify();
        v.set("value", "updated");
        v.save();
        database.deleteRecord(database.lookupByRID(rid, true));
      });

      assertThat(countRecords()).isZero();
    }
  }
}
