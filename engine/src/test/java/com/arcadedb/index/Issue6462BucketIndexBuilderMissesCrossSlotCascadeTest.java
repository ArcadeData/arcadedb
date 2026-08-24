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
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.DatabaseAsyncTask;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6462BucketIndexBuilderMissesCrossSlotCascadeTest extends TestHelper {

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
