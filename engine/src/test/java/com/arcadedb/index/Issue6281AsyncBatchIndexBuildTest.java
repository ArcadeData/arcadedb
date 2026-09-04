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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6281AsyncBatchIndexBuildTest extends TestHelper {
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
    final String bucketName = database.getSchema().getType("V").getBuckets(false).get(0).getName();

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
