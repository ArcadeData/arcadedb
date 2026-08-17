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
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.DatabaseAsyncTask;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue6303AsyncQuiesceTest extends TestHelper {
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
