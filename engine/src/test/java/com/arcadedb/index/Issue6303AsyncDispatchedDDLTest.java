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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6303AsyncDispatchedDDLTest extends TestHelper {
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
   * {@code Issue6324SameTransactionIndexBuildTest} covers the shape on its own terms; it is asserted from here too
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
