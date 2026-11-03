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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.DatabaseAsyncTask;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for index builds that run alongside asynchronous writes, asynchronously dispatched DDL, quiescence
 * and the writer's own open transaction. Each nested class keeps the issue number of the regression it covers in its
 * display name.
 */
class AsyncIndexBuildTest {
  /**
   * Issue #6281: an index is built by SCANNING the buckets, so everything the asynchronous executor was asked to write
   * has to be committed before the build starts. The guard that was supposed to ensure that polled
   * {@code database.isAsyncProcessing()} and drained the executor only when it answered {@code true}.
   * <p>
   * That predicate answers about TASKS, while a worker opens one transaction when it starts and keeps it open across up
   * to {@link com.arcadedb.GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks (10240 by default). An executor whose queues
   * happen to be drained therefore reports itself idle while still holding every record of the current batch
   * uncommitted, the guard was skipped entirely, and the index was built over a bucket that did not contain them. The
   * records then commit afterwards, with no entry ever added for them: the index ends up EMPTY and every lookup answers
   * nothing, silently, on a database that {@code CHECK DATABASE} calls healthy.
   * <p>
   * The barrier is now unconditional ({@link DatabaseInternal#waitForAsyncCompletion()}), because there is no cheap
   * predicate to test first: only {@code waitCompletion()} closes the open batch.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#6281 index build over an idle async executor with an open batch")
  class AsyncBatchIndexBuildTest extends TestHelper {
    private static final int TOT = 200;

    /**
     * The exact shape of the bug: every async task has already RUN - nothing is queued and no worker is executing - and
     * the records are all still uncommitted in the workers' open batch transaction when the index is created.
     */
    @Test
    void anIndexCreatedOverAnIdleAsyncExecutorStillCoversItsUncommittedBatch() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.async().setParallelLevel(2);

      final CountDownLatch executed = new CountDownLatch(TOT);
      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, record -> executed.countDown());
      }

      // Every task has run...
      assertThat(executed.await(30, TimeUnit.SECONDS)).isTrue();
      // ...and the workers are back to parking on their empty queues, which is the state the old guard read as "idle".
      waitForIdleAsyncExecutor();

      // The precondition the whole issue rests on, and the reason "idle" was never the same as "done": not one of
      // those records is committed yet. This is a property of the batching (TOT < ASYNC_TX_BATCH_SIZE), not a race.
      assertThat(database.countType("V", false)).as("the async batch must still be open").isZero();

      database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      database.async().waitCompletion();

      assertIndexCoversEveryRecord();
    }

    /**
     * The same creation with the executor genuinely busy. This path always worked - the old predicate answered
     * {@code true} and the drain ran - and it must keep working now that the drain is unconditional.
     */
    @Test
    void anIndexCreatedWhileTheAsyncExecutorIsBusyCoversEveryRecord() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.async().setParallelLevel(2);

      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, null);
      }

      database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      database.async().waitCompletion();

      assertIndexCoversEveryRecord();
    }

    /**
     * The barrier cannot be satisfied from inside the thing it waits for, so it must REFUSE rather than park there.
     * <p>
     * {@code waitCompletion()} enqueues a marker on every worker - the caller's own included - and blocks until each has
     * run, and the only consumer of a worker's queue is that worker. A worker that reaches the barrier would park on a
     * marker nobody can dequeue and be lost for the life of the process, which is a worse failure than the one the
     * barrier fixes. {@code RebuildIndexStatement.buildIndex} has refused this since issue #2097; the refusal now lives
     * on the barrier itself, so no call site can reach the hang by forgetting to reimplement it.
     * <p>
     * Both bounds here are HANG DETECTORS, not latency bounds - the assertion is the exception, which arrives in
     * milliseconds. Measured with the guard removed: the surefire fork does not merely fail, it never finishes at all
     * (killed after 400 s), because the lost worker takes the database's close with it. That is what the guard turns
     * into a one-line refusal.
     */
    @Test
    @Timeout(60)
    void theBarrierRefusesToBeCalledFromOneOfTheExecutorsOwnWorkers() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.async().setParallelLevel(2);

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<Throwable> raised = new AtomicReference<>();
      database.async().transaction(() -> {
        try {
          // Exactly what an async CREATE INDEX does, minus the SQL layer: this runs ON a worker thread.
          database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        } catch (final Throwable e) {
          raised.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      });

      assertThat(done.await(30, TimeUnit.SECONDS)).as("the worker must come back rather than park on its own marker")
          .isTrue();
      assertThat(raised.get()).as("the barrier must refuse, not hang").isInstanceOf(NeedRetryException.class);
      assertThat(raised.get()).hasMessageContaining("worker threads");

      database.async().waitCompletion();
    }

    /**
     * The same barrier on {@code REBUILD INDEX ... WITH statsOnly = true}, which returns before the rebuild proper and
     * so needs the wait ahead of it rather than after.
     * <p>
     * That branch recomputes the BM25 corpus counters by SCANNING the type and then OVERWRITING the counters with what
     * the scan found. Run against an open async batch it finds nothing committed and writes "0 documents" over counters
     * the records had already bumped when they were saved - so the corpus size stays 0 for a type holding 200
     * documents, and every BM25 score is computed against it, until somebody recomputes again. Unlike the index-entry
     * case this does NOT heal itself, which is why it gets a test and the rebuild path's weaker claim does not.
     */
    @Test
    void aStatsRecomputeOverAnIdleAsyncExecutorStillCountsItsUncommittedBatch() throws Exception {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc").close();
        database.command("sql", "CREATE PROPERTY Doc.content STRING").close();
        database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT").close();
      });

      database.async().setParallelLevel(2);

      final CountDownLatch executed = new CountDownLatch(TOT);
      for (int i = 0; i < TOT; i++) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("content", "java tutorial alpha");
        database.async().createRecord(doc, record -> executed.countDown());
      }

      assertThat(executed.await(30, TimeUnit.SECONDS)).isTrue();
      waitForIdleAsyncExecutor();
      assertThat(database.countType("Doc", false)).as("the async batch must still be open").isZero();

      try (final ResultSet rs = database.command("sql", "REBUILD INDEX `Doc[content]` WITH statsOnly = true")) {
        assertThat(rs.next().<Number>getProperty("statsRecomputed").intValue()).isEqualTo(1);
      }

      database.async().waitCompletion();

      assertThat(database.countType("Doc", false)).isEqualTo(TOT);
      final FullTextIndexMetadata metadata = ((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("Doc[content]")).getIndexesOnBuckets()[0]).getFullTextMetadata();
      assertThat(metadata.getTotalDocs()).as(
          "the corpus size BM25 scores against must be the whole corpus, not what happened to be committed").isEqualTo(TOT);
      assertThat(metadata.getSumDocLength()).as("and so must the length total it averages").isPositive();
    }

    /**
     * The fourth way into a bucket scan: {@code Schema.buildBucketIndex(...)} on its own.
     * <p>
     * {@code TypeIndexBuilder} pays the barrier before delegating down to this builder, and so does
     * {@code REBUILD INDEX} - but it is public API in its own right, and {@code CHECK DATABASE ... FIX} rebuilds a
     * damaged index through it. Reached that way it has to pay the barrier itself, or it produces exactly the
     * silently incomplete index this issue is about.
     */
    @Test
    void aBucketIndexBuiltDirectlyOverAnIdleAsyncExecutorStillCoversItsUncommittedBatch() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      final String bucketName = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

      database.async().setParallelLevel(2);

      final CountDownLatch executed = new CountDownLatch(TOT);
      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, record -> executed.countDown());
      }

      assertThat(executed.await(30, TimeUnit.SECONDS)).isTrue();
      waitForIdleAsyncExecutor();
      assertThat(database.countType("V", false)).as("the async batch must still be open").isZero();

      final Index bucketIndex = database.getSchema().buildBucketIndex("V", bucketName, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

      database.async().waitCompletion();

      assertThat(database.countType("V", false)).isEqualTo(TOT);
      assertThat(bucketIndex.countEntries()).as("a directly built bucket index must cover every record of its bucket")
          .isEqualTo(TOT);
    }

    private void assertIndexCoversEveryRecord() {
      assertThat(database.countType("V", false)).isEqualTo(TOT);

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).as("every record must have an entry in the index").isEqualTo(TOT);

      for (int i = 0; i < TOT; i++)
        try (final ResultSet rs = database.query("sql", "select from V where id = ?", i)) {
          assertThat(rs.hasNext()).as("record id " + i + " must be reachable through the index").isTrue();
          assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(i);
        }
    }

    /**
     * Settles on the state the old guard mistook for "everything is written": queues empty, no task executing. Bounded
     * because it is a set-up step and not what is under test - if the executor never settles the assertions below still
     * hold, they just stop reproducing the bug.
     */
    private void waitForIdleAsyncExecutor() throws InterruptedException {
      final DatabaseInternal db = (DatabaseInternal) database;
      for (int i = 0; i < 500 && db.isAsyncProcessing(); i++)
        Thread.sleep(10);
    }
  }

  /**
   * Regression test for issue #6303, item 3: DDL dispatched through the asynchronous API has to WORK, not merely fail
   * clearly.
   * <p>
   * {@code POST /command} with {@code awaitResponse=false} is a documented way to fire a long DDL statement, and index
   * creation is exactly the kind of long DDL somebody would fire that way. Until #6281 it hung: the command ran on one
   * of the async executor's own workers, the barrier it needs enqueues a marker on every worker INCLUDING the caller's
   * own, and the only consumer of a worker's queue is that worker - so the thread parked for ever. #6281 turned the
   * hang into a {@code NeedRetryException} and a workaround, which is a strict improvement and still not the operation.
   * <p>
   * A dispatched statement that parses to DDL now runs on {@code AsyncCommandPool}, whose threads are deliberately not
   * workers of any executor, so the barrier can be satisfied instead of refused. Everything else stays on the workers,
   * which is not caution but a requirement - see {@code anOrdinaryWriteCommandStillRunsOnAWorker} below. The guard of
   * #6281 stays as the backstop for anything that reaches a worker anyway - {@code Issue6303AsyncQuiesceTest} pins that
   * it still refuses.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#6303 DDL dispatched asynchronously")
  class AsyncDispatchedDDLTest extends TestHelper {
    private static final int TOT = 200;

    /**
     * The reported shape: {@code CREATE INDEX} sent without waiting for the response. It must build the index, not
     * refuse.
     */
    @Test
    @Timeout(180)
    void createIndexDispatchedAsynchronouslyBuildsTheIndex() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      database.async().setParallelLevel(2);

      final CountDownLatch inserted = new CountDownLatch(TOT);
      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, record -> inserted.countDown());
      }
      assertThat(inserted.await(60, TimeUnit.SECONDS)).isTrue();

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<Exception> failure = new AtomicReference<>();
      database.async().command("sql", "CREATE INDEX ON V (id) UNIQUE", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          done.countDown();
        }

        @Override
        public void onError(final Exception exception) {
          failure.compareAndSet(null, exception);
          done.countDown();
        }
      });

      assertThat(done.await(120, TimeUnit.SECONDS)).as("the command must run to an answer, not park").isTrue();
      assertThat(failure.get()).as("the barrier is satisfiable off the workers, so there is nothing to refuse").isNull();

      database.async().waitCompletion();

      assertThat(database.countType("V", false)).isEqualTo(TOT);
      assertThat(database.getSchema().getIndexByName("V[id]").countEntries()).as(
          "an index built by an asynchronously dispatched command must cover every record like any other").isEqualTo(TOT);
    }

    /** The same for {@code REBUILD INDEX}, which has refused this since issue #2097. */
    @Test
    @Timeout(180)
    void rebuildIndexDispatchedAsynchronouslyRebuildsTheIndex() throws Exception {
      database.transaction(() -> {
        database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER);
        database.command("sql", "CREATE INDEX ON V (id) UNIQUE").close();
      });
      database.async().setParallelLevel(2);

      final CountDownLatch inserted = new CountDownLatch(TOT);
      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, record -> inserted.countDown());
      }
      assertThat(inserted.await(60, TimeUnit.SECONDS)).isTrue();

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<Exception> failure = new AtomicReference<>();
      database.async().command("sql", "REBUILD INDEX `V[id]`", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          done.countDown();
        }

        @Override
        public void onError(final Exception exception) {
          failure.compareAndSet(null, exception);
          done.countDown();
        }
      });

      assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
      assertThat(failure.get()).isNull();

      database.async().waitCompletion();

      assertThat(database.getSchema().getIndexByName("V[id]").countEntries()).isEqualTo(TOT);
    }

    /**
     * The property moving DDL off the workers would otherwise take away without saying so:
     * {@code async().command(...)} followed by {@code async().waitCompletion()} still means "the command has finished".
     * A dispatched DDL statement is no longer in any worker's queue, so the wait had to learn to count it separately.
     */
    @Test
    @Timeout(120)
    void waitCompletionStillCoversAnAsynchronouslyDispatchedDDLStatement() {
      for (int i = 0; i < 20; i++)
        database.async().command("sql", "CREATE DOCUMENT TYPE Dispatched" + i, (AsyncResultsetCallback) null);

      database.async().waitCompletion();

      for (int i = 0; i < 20; i++)
        assertThat(database.getSchema().existsType("Dispatched" + i)).as(
            "waitCompletion() must not return before the dispatched DDL has run").isTrue();
    }

    /**
     * A script routes as a whole, on whether ANY statement in it is DDL - so an `INSERT` sharing a script with a
     * `CREATE INDEX` goes off the workers with it. That is the only arrangement that works: the statements share one
     * transaction and one thread, so the script cannot be half here and half there, and it is the DDL half that
     * dictates where both can run.
     * <p>
     * The entry for the record the script itself inserted is asserted here as of issue #6324, item 1. It used to be
     * missing, and this test used to say so in a comment instead of asserting it: both statements share one
     * transaction, so the record was still uncommitted - and therefore invisible to the build's scan - at the moment
     * the index was created, having been saved before the index existed to stage an entry for. The build now runs
     * INSIDE the transaction that is writing rather than opening one of its own, which is what lets the scan see it.
     * {@code SameTransactionIndexBuildTest} covers the shape on its own terms; it is asserted from here too
     * because this is a route a user actually takes to it.
     */
    @Test
    @Timeout(180)
    void aScriptMixingDDLAndWritesRunsOffTheWorkersAsAWhole() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<String> ranOn = new AtomicReference<>();
      final AtomicReference<Exception> failure = new AtomicReference<>();
      database.async().command("sqlscript", "INSERT INTO V SET id = 7; CREATE INDEX ON V (id) UNIQUE;",
          new AsyncResultsetCallback() {
            @Override
            public void onComplete(final ResultSet rs) {
              ranOn.set(Thread.currentThread().getName());
              done.countDown();
            }

            @Override
            public void onError(final Exception exception) {
              failure.compareAndSet(null, exception);
              done.countDown();
            }
          });

      assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
      assertThat(failure.get()).as("the barrier the CREATE INDEX needs must be satisfiable where the script ran").isNull();
      assertThat(ranOn.get()).as("a script carrying DDL goes to the pool whole, insert included")
          .startsWith("ArcadeDB-AsyncCommand-");

      database.async().waitCompletion();

      assertThat(database.countType("V", false)).as("and the non-DDL half of the script still did its work")
          .isEqualTo(1);
      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index).as("as did the DDL half").isNotNull();
      assertThat(index.countEntries()).as("and the index knows about the record its own script inserted (#6324)")
          .isEqualTo(1);
    }

    /**
     * The other half of the routing, and the half that is easy to break by widening the fix: everything that is NOT
     * DDL keeps running on the workers. A worker owns a batch transaction and is the unit
     * {@code ThreadBucketSelectionStrategy} pins a bucket to, so "as many workers as buckets" is a documented way to
     * make concurrent asynchronous writers never contend - arithmetic a JVM-wide pool sized for the machine cannot
     * honour, and whose loss shows up as MVCC conflicts rather than as an error anybody would connect to a dispatch
     * change ({@code AsyncInsertTest}).
     */
    @Test
    @Timeout(120)
    void anOrdinaryWriteCommandStillRunsOnAWorker() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      final AtomicReference<String> ranOn = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);
      database.async().command("sql", "INSERT INTO V SET id = 1", new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet rs) {
          ranOn.set(Thread.currentThread().getName());
          done.countDown();
        }

        @Override
        public void onError(final Exception exception) {
          ranOn.set("error: " + exception);
          done.countDown();
        }
      });

      assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
      assertThat(ranOn.get()).as("a non-DDL command must stay on an async worker, batch transaction and bucket pinning included")
          .startsWith("AsyncExecutor-");

      database.async().waitCompletion();
      assertThat(database.countType("V", false)).isEqualTo(1);
    }
  }

  /**
   * Regression test for issue #6303, item 2: the worker pause an index build relies on used to be scheduled and
   * forgotten.
   * <p>
   * {@code BucketIndexBuilder} put a park task on every worker and discarded the boolean {@code scheduleTask} returns,
   * so nothing confirmed a worker had reached it before the scan began, and the task did not commit - a worker could
   * park holding an uncommitted batch. Both halves matter: the barrier of #6281 makes the scan see what the async side
   * wrote BEFORE the build, and the pause is what is supposed to stop it writing DURING it. A record written in the gap
   * is in neither the scan nor the index, because it was saved before the index existed and staged no entry for it.
   * <p>
   * {@code DatabaseInternal.quiesceAsync()} is the gated form: commit, confirm, hold. These tests pin its contract
   * directly rather than through a build, because the window a build leaves is a few instructions wide and cannot be
   * hit on demand - what CAN be pinned is that no such window exists while the quiescence is held.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#6303 async quiescence")
  class AsyncQuiesceTest extends TestHelper {
    private static final int TOT = 200;
    /** Long enough to be conclusive about a task that must NOT run, short enough not to dominate the class. */
    private static final long MUST_NOT_HAPPEN_MILLIS = 500;

    /**
     * The commit half. The park task commits the worker's open batch, so a caller holding the quiescence reads
     * everything the async API was asked to write - the state #6281 showed an index build cannot afford to be wrong
     * about.
     */
    @Test
    @Timeout(120)
    void quiescingCommitsTheOpenBatchOfEveryWorker() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      database.async().setParallelLevel(2);

      final CountDownLatch executed = new CountDownLatch(TOT);
      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, record -> executed.countDown());
      }
      assertThat(executed.await(30, TimeUnit.SECONDS)).isTrue();

      // The precondition: the tasks have all RUN and not one record is committed. A property of the batching
      // (TOT < ASYNC_TX_BATCH_SIZE), not a race.
      waitForIdleAsyncExecutor();
      assertThat(database.countType("V", false)).as("the async batch must still be open").isZero();

      try (final AsyncQuiesce quiesce = ((DatabaseInternal) database).quiesceAsync()) {
        assertThat(database.countType("V", false)).as(
            "a parked worker still holding its batch would be a pause that freezes the writes without publishing them")
            .isEqualTo(TOT);
      }
    }

    /**
     * The confirm-and-hold half, and the one the discarded boolean was about: while the quiescence is held, a task
     * submitted to a worker does not run. Before this, the build proceeded whether or not any worker had reached the
     * pause, so a task already queued ahead of it wrote records underneath the scan.
     */
    @Test
    @Timeout(120)
    void nothingRunsOnAWorkerWhileTheQuiescenceIsHeld() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      database.async().setParallelLevel(2);
      // Force the executor to exist and settle before the quiescence, so what the test measures is the hold and not
      // the lazy start-up.
      database.async().waitCompletion();

      final CountDownLatch ran = new CountDownLatch(1);

      try (final AsyncQuiesce quiesce = ((DatabaseInternal) database).quiesceAsync()) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", 1);
        database.async().createRecord(v, record -> ran.countDown());

        assertThat(ran.await(MUST_NOT_HAPPEN_MILLIS, TimeUnit.MILLISECONDS)).as(
            "every worker is parked: a record written here is one an in-progress scan would miss").isFalse();
      }

      assertThat(ran.await(60, TimeUnit.SECONDS)).as("and the workers must be released when the handle is closed")
          .isTrue();
      database.async().waitCompletion();
      assertThat(database.countType("V", false)).isEqualTo(1);
    }

    /**
     * Nested quiescence on the same thread must ride on the outer one rather than parking workers that are already
     * parked and could never run a second park task. {@code REBUILD INDEX} quiescing and then calling a builder that
     * quiesces again is exactly this shape.
     */
    @Test
    @Timeout(120)
    void aNestedQuiescenceRidesOnTheOuterOneInsteadOfDeadlocking() throws Exception {
      database.async().setParallelLevel(2);
      database.async().waitCompletion();

      final DatabaseInternal db = (DatabaseInternal) database;
      try (final AsyncQuiesce outer = db.quiesceAsync()) {
        try (final AsyncQuiesce inner = db.quiesceAsync()) {
          assertThat(inner).isNotNull();
        }
        // The inner close must NOT have released the workers: the outer one still owns the quiescence.
        final CountDownLatch ran = new CountDownLatch(1);
        ((DatabaseAsyncExecutorImpl) database.async()).scheduleTask(0, new CountingTask(ran), true, 0);
        assertThat(ran.await(MUST_NOT_HAPPEN_MILLIS, TimeUnit.MILLISECONDS)).as(
            "closing the inner handle must not end the quiescence the outer one is holding").isFalse();
      }

      database.async().waitCompletion();
    }

    /**
     * A database that never used the async API must not grow a set of worker threads just to have them parked. The old
     * call site asked {@code database.async()} for the thread count, and that accessor CREATES the executor.
     */
    @Test
    @Timeout(120)
    void quiescingADatabaseThatNeverUsedAsyncCreatesNoExecutor() {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final AsyncQuiesce quiesce = db.quiesceAsync()) {
        assertThat(quiesce).isNotNull();
      }
      assertThat(db.isAsyncProcessing()).as("nothing was created, so there is nothing processing").isFalse();
    }

    /**
     * Refused from one of the executor's own workers, for the same reason the barrier is: the park would be enqueued on
     * the caller's own queue and only the caller drains it.
     */
    @Test
    @Timeout(120)
    void quiescingIsRefusedFromOneOfTheExecutorsOwnWorkers() throws Exception {
      database.async().setParallelLevel(2);

      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<Throwable> raised = new AtomicReference<>();
      database.async().transaction(() -> {
        try (final AsyncQuiesce quiesce = ((DatabaseInternal) database).quiesceAsync()) {
          raised.compareAndSet(null, new IllegalStateException("the quiescence must not be granted here"));
        } catch (final Throwable e) {
          raised.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      });

      assertThat(done.await(60, TimeUnit.SECONDS)).as("the worker must come back rather than park on its own task")
          .isTrue();
      assertThat(raised.get()).isInstanceOf(NeedRetryException.class);
      assertThat(raised.get()).hasMessageContaining("worker threads");

      database.async().waitCompletion();
    }

    /**
     * An index built while the async executor is genuinely busy must still cover every record. This is the end-to-end
     * shape the quiescence exists for: the build now holds the workers rather than racing them.
     */
    @Test
    @Timeout(180)
    void anIndexBuiltWhileTheExecutorIsBusyCoversEveryRecord() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      database.async().setParallelLevel(2);

      for (int i = 0; i < TOT; i++) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        database.async().createRecord(v, null);
      }

      database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      database.async().waitCompletion();

      assertThat(database.countType("V", false)).isEqualTo(TOT);
      assertThat(database.getSchema().getIndexByName("V[id]").countEntries()).as(
          "every record must have an entry, whatever the async side was doing while the index was built").isEqualTo(TOT);
    }

    /**
     * A quiescence that gives up must not leave behind the workers that DID park. They are waiting on a latch nobody
     * else owns, so failing without counting it down would lose them until the database closes - a worse outcome than
     * the refusal that got us there, and one that only shows up on the failure path nothing normally exercises.
     * <p>
     * The wedge below is what a worker busy inside a long user task looks like from the quiescing side. The timeout is
     * shrunk through the stall-window setting the quiescence budget is derived from, so this stays a bounded test
     * rather than a one-minute one.
     */
    @Test
    @Timeout(180)
    void aQuiescenceThatTimesOutReleasesTheWorkersThatDidPark() throws Exception {
      database.async().setParallelLevel(2);
      database.async().waitCompletion();

      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
      async.setCheckForStalledQueuesMaxDelay(50);

      final CountDownLatch wedgeStarted = new CountDownLatch(1);
      final CountDownLatch releaseWedge = new CountDownLatch(1);
      async.scheduleTask(0, new WedgeTask(wedgeStarted, releaseWedge), true, 0);
      assertThat(wedgeStarted.await(60, TimeUnit.SECONDS)).isTrue();

      try {
        assertThatThrownBy(() -> ((DatabaseInternal) database).quiesceAsync()).as(
                "a worker that never parks must refuse the quiescence rather than let the scan run under it")
            .isInstanceOf(NeedRetryException.class).hasMessageContaining("did not quiesce");

        // Worker 1 parked long before the timeout: it has to be walking again, or it is lost.
        final CountDownLatch ran = new CountDownLatch(1);
        async.scheduleTask(1, new CountingTask(ran), true, 0);
        assertThat(ran.await(60, TimeUnit.SECONDS)).as(
            "the workers that parked must be released when the quiescence gives up").isTrue();

        // And the lock is back, so the next attempt is not refused by the failed one still holding it.
        releaseWedge.countDown();
        try (final AsyncQuiesce retry = ((DatabaseInternal) database).quiesceAsync()) {
          assertThat(retry).isNotNull();
        }
      } finally {
        releaseWedge.countDown();
        database.async().waitCompletion();
      }
    }

    private void waitForIdleAsyncExecutor() throws InterruptedException {
      final DatabaseInternal db = (DatabaseInternal) database;
      for (int i = 0; i < 500 && db.isAsyncProcessing(); i++)
        Thread.sleep(10);
    }

    /** Occupies a worker until released: what "busy inside a long user task" looks like from the quiescing side. */
    private record WedgeTask(CountDownLatch started, CountDownLatch release) implements DatabaseAsyncTask {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
        started.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }

    /** Counts down when the worker runs it: the probe for "is this worker taking work right now?". */
    private record CountingTask(CountDownLatch ran) implements DatabaseAsyncTask {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
        ran.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }
  }

  /**
   * Regression test for issue #6324, item 1: a {@code CREATE INDEX} that runs in the SAME transaction as the writes it
   * is supposed to index must index them.
   * <p>
   * The build scans the buckets, and a record written by the transaction the build runs in is not on disk yet - so it is
   * not in the scan. It cannot fall back on staging its own index operation either, because it was saved BEFORE the
   * index existed to stage one for. The result was an index that is readable, reported healthy by {@code CHECK DATABASE}
   * and answers {@code WHERE id = 7} with nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#6324 index build inside the writer transaction")
  class SameTransactionIndexBuildTest extends TestHelper {

    /** The reported shape, verbatim: one script, one transaction, an INSERT then a CREATE INDEX over it. */
    @Test
    @Timeout(60)
    void aScriptThatInsertsThenIndexesInOneTransactionIndexesWhatItInserted() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.transaction(
          () -> database.command("sqlscript", "INSERT INTO V SET id = 7; CREATE INDEX ON V (id) UNIQUE;").close());

      assertThat(database.countType("V", false)).as("the record is there").isEqualTo(1);

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).as("and so is its index entry").isEqualTo(1);
      assertThat(index.get(new Object[] { 7 }).hasNext()).as("an index that answers the lookup it exists for").isTrue();
    }

    /** The same through the API rather than through SQL: {@code createTypeIndex} inside an open transaction. */
    @Test
    @Timeout(60)
    void aTypeIndexCreatedInsideTheWritersTransactionIndexesTheUncommittedRecords() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.transaction(() -> {
        for (int i = 0; i < 10; i++) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.save();
        }
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      });

      assertThat(database.countType("V", false)).isEqualTo(10);

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).isEqualTo(10);
      for (int i = 0; i < 10; i++)
        assertThat(index.get(new Object[] { i }).hasNext()).as("id " + i + " must be found through the index").isTrue();
    }

    /**
     * The build must still see what the transaction DELETED, not only what it inserted: a record removed in the same
     * transaction has no business getting an index entry from a scan that has not caught up with the removal.
     */
    @Test
    @Timeout(60)
    void aRecordDeletedInTheSameTransactionGetsNoIndexEntry() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.transaction(() -> {
        for (int i = 0; i < 4; i++) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.save();
        }
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE id = 2")) {
          rs.next().getRecord().get().asDocument().getRecord().delete();
        }
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      });

      assertThat(database.countType("V", false)).isEqualTo(3);

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).as("the deleted record must not leave a phantom entry behind").isEqualTo(3);
      assertThat(index.get(new Object[] { 2 }).hasNext()).as("and must not be answerable through the index").isFalse();
    }

    /**
     * And a record UPDATED in the same transaction must be indexed under its new key, not the one still on disk.
     */
    @Test
    @Timeout(60)
    void aRecordUpdatedInTheSameTransactionIsIndexedUnderItsNewKey() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.transaction(() -> {
        final MutableDocument v = database.newDocument("V");
        v.set("id", 1);
        v.save();
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE id = 1")) {
          final MutableDocument v = rs.next().getRecord().get().asDocument().modify();
          v.set("id", 42);
          v.save();
        }
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      });

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).isEqualTo(1);
      // Asked of the INDEX and not of a SELECT, which would answer the same from a full scan and so would pass
      // whatever the index holds.
      assertThat(index.get(new Object[] { 42 }).hasNext()).as("the new key is the one the index answers on").isTrue();
      assertThat(index.get(new Object[] { 1 }).hasNext()).as("and the old key is not").isFalse();
    }

    /**
     * The other half of the split: the index COMPONENT is committed on its own, whatever the caller's transaction goes
     * on to do.
     * <p>
     * The schema entry naming the index is written by {@code recordFileChanges} as soon as the statement returns and
     * nothing later takes it back, so an index whose first page was left in a transaction that then rolled back would
     * be a schema entry pointing at a file with no pages - and the next write to it fails with "the file is invalid",
     * on a database that has been reopened since and has no idea why.
     */
    @Test
    @Timeout(60)
    void anIndexCreatedInATransactionThatRollsBackIsStillUsable() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.begin();
      final MutableDocument doomed = database.newDocument("V");
      doomed.set("id", 1);
      doomed.save();
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      database.rollback();

      assertThat(database.countType("V", false)).as("the record went with the rollback").isZero();

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index).as("the index the schema still names must still be there").isNotNull();
      assertThat(index.countEntries()).as("and empty, like the type").isZero();

      // The point of the test: the index is WRITEABLE afterwards, not merely present.
      database.transaction(() -> {
        final MutableDocument v = database.newDocument("V");
        v.set("id", 7);
        v.save();
      });
      assertThat(index.get(new Object[] { 7 }).hasNext()).isTrue();
    }

    /**
     * A NESTED transaction cannot see an outer one's uncommitted pages, so an index created in the outer transaction
     * has to be committed to be usable from inside a nested one - which is what the component half of the split is
     * for. The shape is ordinary enough to reach by accident: any test or handler running with an ambient transaction
     * and a {@code begin()}/{@code commit()} pair inside it.
     */
    @Test
    @Timeout(60)
    void anIndexCreatedInAnOuterTransactionIsWriteableFromANestedOne() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      database.begin();
      try {
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");

        database.begin();
        final MutableDocument v = database.newDocument("V");
        v.set("id", 3);
        v.save();
        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.commit();
      }

      assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { 3 }).hasNext()).isTrue();
    }

    /**
     * The permission to commit is a parameter of the build's own, never a value of its batch size.
     * <p>
     * It used to be spelled "batch size 0", which collides with a user-supplied
     * {@code REBUILD INDEX ... WITH batchSize = 0}: for the vector families, which do not chunk by record count at all,
     * that literal zero would have been read as the permission and turned off the byte-based chunking for the whole
     * rebuild. The statement refuses the value now, which is the honest answer for every index family - a batch size
     * below one is not a smaller batch, it is a request that cannot be honoured.
     */
    @Test
    @Timeout(60)
    void aNonPositiveRebuildBatchSizeIsRefusedRatherThanReadAsAPermission() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      database.transaction(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id"));
      database.transaction(() -> {
        final MutableDocument v = database.newDocument("V");
        v.set("id", 1);
        v.save();
      });

      assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = 0").close())
          .hasMessageContaining("batchSize");
      assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = -1").close())
          .hasMessageContaining("batchSize");
      assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `V[id]` WITH maxAttempts = 0").close())
          .hasMessageContaining("maxAttempts");

      // A legal one still rebuilds, so the guard above is not simply refusing everything.
      database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = 1").close();
      assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { 1 }).hasNext()).isTrue();
    }

    /**
     * An EMPTY open transaction is not a reason to share it, and this is the case that makes the distinction matter.
     * <p>
     * {@code DatabaseAsyncExecutorImpl.runCommand} opens a transaction around every dispatched command, because
     * {@code requiresActiveTx()} defaults to true. Keying the decision on "is a transaction active" alone would
     * therefore make a {@code CREATE INDEX} sent with {@code awaitResponse=false} - the case #6303 item 3 and #6324
     * item 5 exist to make work - share a transaction that holds nothing, and silently give up the chunked commit its
     * batch size is meant to guarantee: one uncommitted transaction from the first record to the last. Sharing is
     * asked of what the transaction CONTAINS, not of whether one exists.
     */
    @Test
    @Timeout(60)
    void anEmptyOpenTransactionIsNotSharedSoTheBuildKeepsItsChunkedCommit() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));
      database.transaction(() -> {
        for (int i = 0; i < 40; i++) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.save();
        }
      });

      // Open, write nothing, build. The build must NOT join: with a batch size of 10 over 40 committed records it
      // chunk-commits, which it can only do on a transaction of its own.
      database.begin();
      final TransactionContext outer = ((DatabaseInternal) database).getTransactionIfExists();
      assertThat(outer.hasChanges()).as("precondition: the transaction the build is offered is empty").isFalse();

      database.getSchema().buildTypeIndex("V", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true).withBatchSize(10).create();

      assertThat(((DatabaseInternal) database).getTransactionIfExists().hasChanges())
          .as("a build that did not join left the caller's transaction as empty as it found it").isFalse();
      database.commit();

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).isEqualTo(40);
      for (int i = 0; i < 40; i++)
        assertThat(index.get(new Object[] { i }).hasNext()).as("id " + i).isTrue();
    }

    /**
     * A build that fails must not leave a registered, {@code AVAILABLE}, half-populated index behind - and the failure
     * that gets there is a {@link com.arcadedb.exception.DuplicatedKeyException} raised mid-scan, which is exactly what
     * a unique build can now hit, since it sees the caller's own pending writes.
     * <p>
     * The path matters: inside a JOINED transaction {@code LocalDatabase.transaction} rethrows a retryable exception
     * immediately rather than calling the error callback (issue #661 - retrying would roll back a transaction it does
     * not own), so a cleanup hung off that callback never runs.
     */
    @Test
    @Timeout(60)
    void aBuildThatFailsOnACallersDuplicateLeavesNoIndexBehind() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      assertThatThrownBy(() -> database.transaction(() -> {
        for (int i = 0; i < 2; i++) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", 7);
          v.save();
        }
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      })).isInstanceOf(Exception.class);

      assertThat(database.getSchema().existsIndex("V[id]"))
          .as("an index that could not be built must be gone, not registered and empty").isFalse();
    }

    /**
     * The same through {@code BucketIndexBuilder}, which is the path that actually lacked the cleanup: it has no
     * try/catch of its own around the build, unlike {@code TypeIndexBuilder}, so it was relying entirely on the
     * transaction's error callback - the one {@code LocalDatabase.transaction} skips for a retryable exception raised
     * inside a joined transaction (issue #661). This is how {@code CHECK DATABASE ... FIX} rebuilds a damaged index.
     */
    @Test
    @Timeout(60)
    void aBucketIndexBuildThatFailsOnACallersDuplicateLeavesNoIndexBehind() {
      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      assertThatThrownBy(() -> database.transaction(() -> {
        for (int i = 0; i < 2; i++) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", 7);
          v.save();
        }
        database.getSchema().buildBucketIndex("V", "V_0", new String[] { "id" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      })).isInstanceOf(Exception.class);

      assertThat(database.getSchema().existsIndex("V[id]"))
          .as("the half-built sub-index must be dropped, not left registered and AVAILABLE").isFalse();
      assertThat(database.getSchema().getIndexes())
          .as("and it must not survive under its own bucket name either")
          .noneMatch(i -> i.getName().startsWith("V_0_"));
    }
  }

  /**
   * Regression test for #6462, the {@code BucketIndexBuilder} half: {@code quiesceAsync()} parks each
   * worker once its OWN queue is drained, but a task still mid-flight on a DIFFERENT worker can
   * schedule a follow-up onto an already-parked worker's queue from inside its own execution - exactly
   * what a bidirectional cross-slot edge does ({@code DatabaseAsyncExecutorImpl#newEdge} schedules the
   * incoming-edge task onto the destination slot from inside the source task's own callback). The
   * follow-up then sits queued behind the park, uncommitted and invisible, while a
   * {@code BucketIndexBuilder} scan concurrent with it runs anyway - the built index misses the entry.
   * <p>
   * Reduced to a plain task scheduled directly at the slot level (as
   * {@code AsyncCrossSlotSchedulingDeadlockTest} and {@code Issue6303AsyncQuiesceTest} already do)
   * rather than through the graph API: a task on one worker, still blocked, that - once released -
   * creates a record targeting the OTHER worker's bucket from inside its own execution, mirroring the
   * shape exactly without depending on which slot a given RID happens to hash to.
   */
  @Nested
  @DisplayName("#6462 bucket index build must not miss a cross-slot cascade in flight")
  class CrossSlotCascadeTest extends TestHelper {

    @Test
    @Timeout(60)
    void bucketIndexBuildMustNotMissARecordFromACrossSlotCascadeStillInFlight() throws Exception {
      final DatabaseInternal            db    = (DatabaseInternal) database;
      final DatabaseAsyncExecutorImpl   async = (DatabaseAsyncExecutorImpl) db.async();
      async.setParallelLevel(2);

      database.transaction(() -> database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER));

      final Bucket vBucket  = database.getSchema().getType("V").getBuckets(false).get(0);
      final int    destSlot = async.getSlot(vBucket.getFileId());
      final int    sourceSlot = 1 - destSlot;

      final CountDownLatch sourceTaskRunning = new CountDownLatch(1);
      final CountDownLatch releaseSourceTask = new CountDownLatch(1);
      final CountDownLatch cascadeCreated    = new CountDownLatch(1);

      // SOURCE TASK on the OTHER slot - mirrors CreateEdgeAsyncTask: once released, it creates a
      // record targeting the index's own bucket FROM INSIDE its own execution, exactly as newEdge()'s
      // cross-slot callback schedules CreateIncomingEdgeAsyncTask onto the destination slot.
      async.scheduleTask(sourceSlot, new DatabaseAsyncTask() {
        @Override
        public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
          sourceTaskRunning.countDown();
          awaitLatch(releaseSourceTask);

          final MutableDocument v = database.newDocument("V");
          v.set("id", 42);
          database.async().createRecord(v, record -> cascadeCreated.countDown());
        }

        @Override
        public boolean requiresActiveTx() {
          return false;
        }
      }, true, 0);

      assertThat(sourceTaskRunning.await(5, TimeUnit.SECONDS)).isTrue();

      // Runs the build concurrently, on its own thread: it must block until the source task (still
      // held below) and whatever it schedules have fully drained.
      final AtomicReference<com.arcadedb.index.Index> builtIndex = new AtomicReference<>();
      final CountDownLatch                            buildDone  = new CountDownLatch(1);
      final Thread builder = new Thread(() -> {
        try {
          builtIndex.set(database.getSchema().buildBucketIndex("V", vBucket.getName(), new String[] { "id" })
              .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create());
        } finally {
          buildDone.countDown();
        }
      }, "index-builder");
      builder.setDaemon(true);
      builder.start();

      // Widens the window a real cross-slot edge would leave open: with the pre-#6462-fix code,
      // quiesceAsync() parks the (idle, nothing queued yet) destination worker essentially instantly
      // once the build starts above - far under this bound - so releasing the source task only after
      // this sleep reliably reproduces the race rather than depending on it by luck.
      Thread.sleep(300);

      releaseSourceTask.countDown();

      // NOT gated on cascadeCreated: that latch only counts down once the cross-slot record's own async
      // task has actually EXECUTED and committed - which, for the very race under test, may not happen
      // until after quiesceAsync() has already released the workers. Waiting for it first would silently
      // wait out the bug. buildDone is the only synchronization point that matters here: create() cannot
      // even reach its quiesceAsync() call's parked.await() until the source task has finished (its own
      // trailing park sits queued behind it), so by the time create() returns, the cross-slot record has
      // at least been SCHEDULED - the question the assertions below answer is whether it was also seen.
      assertThat(buildDone.await(30, TimeUnit.SECONDS)).as("the index build must complete").isTrue();
      builder.join(5000);

      // THE ASSERTION: a record whose cross-slot creation was already scheduled before the build even
      // started quiescing must be visible in both the type and the freshly built index the INSTANT
      // create() returns - not eventually, once some later drain happens to catch it up. That "eventually"
      // is exactly the silently incomplete index #6462 is about: a caller reading right after the DDL
      // statement returns must not see a database that has not caught up with its own recent past.
      assertThat(builtIndex.get()).as("the build must have produced an index").isNotNull();
      assertThat(database.countType("V", false))
          .as("a record whose cross-slot creation was already scheduled before the build started quiescing must be "
              + "committed by the time the build - a DDL statement - returns")
          .isEqualTo(1);
      assertThat(builtIndex.get().countEntries())
          .as("a record created by a cross-slot cascade still in flight when the build started must not be missing "
              + "from the built index")
          .isEqualTo(1);

      // Sanity: the record is not permanently lost either way - only the timing guarantee is under test.
      assertThat(cascadeCreated.await(10, TimeUnit.SECONDS)).as("the cross-slot record must eventually be created")
          .isTrue();
    }

    private static void awaitLatch(final CountDownLatch latch) {
      try {
        if (!latch.await(20, TimeUnit.SECONDS))
          throw new AssertionError("Latch never released within 20s");
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
  }
}
