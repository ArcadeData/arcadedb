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

import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.WALFile;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6526, the three follow-ups the 14-round review of #6511 left explicitly out of scope.
 * <ol>
 *   <li><b>{@code setParallelLevel()} tore down the whole pool.</b> It was the one caller of {@code createThreads()}
 *   left on a runtime-callable path, so it kept both failure modes #6511 removed from the durability setters: the
 *   published worker array was nulled for the entire teardown - an unrelated concurrent producer got
 *   {@code "Async executor has been shut down"}, the #6505 incident - and a worker whose queue was full refused the
 *   exit marker and was interrupted, which notified somebody else's queued tasks as completed WITHOUT running
 *   them.</li>
 *   <li><b>The DDL dispatch path ignored the executor's durability policy.</b> A command routed to
 *   {@code AsyncCommandPool} (an {@code awaitResponse=false} {@code CREATE INDEX}, since #6303) read neither
 *   {@code transactionUseWAL} nor {@code transactionSync}, so it silently committed under whatever ambient flags it
 *   found - a latent exception to the per-task contract #6511 established for every other task.</li>
 *   <li><b>Nothing counted the boundary commits</b> #6511's fix forces when a durability flag changes under an open
 *   batch, so the batching degrading toward {@code commitEvery=1} was invisible until somebody read seven hours of
 *   logs.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6526AsyncExecutorFollowUpsTest extends TestHelper {

  /**
   * How long the gate task below occupies its worker. Longer than the one second
   * {@code shutdownThreadsLocked()} waits for its exit marker before escalating to {@code interrupt()}, so the
   * regression this test covers has time to happen if the teardown ever comes back. Not a latency assertion:
   * nothing here measures elapsed time, this only has to outlive a fixed timeout in the code under test.
   */
  private static final long WORKER_HOLD_MS = 3_000;
  // The two methods that hold a worker for it carry @Tag("slow") and run in the slow lane; the rest of the class
  // stays in the default one, which is the per-method split CLAUDE.md asks for when only some methods are slow.

  /**
   * How long the gate task of the {@code kill()} test occupies its worker: long enough that only an interrupt can
   * end it, so the test cannot pass by the worker simply finishing on its own. Never actually waited for - a
   * {@code kill()} that does its job cuts it short in milliseconds.
   */
  private static final long UNTIL_INTERRUPTED_MS = 120_000;

  /**
   * Item 1, the half that LOSES DATA. Tasks already queued on a worker the resize retires must still run, in the
   * batch transaction that worker already owns. Before the fix the retired worker was interrupted whenever it could
   * not take the exit marker within a second, and its queue was drained straight into {@code completed()} - waiters
   * released, work silently gone.
   */
  @Test
  @Tag("slow")
  @Timeout(60)
  void loweringTheParallelLevelRunsTheWorkStillQueuedOnTheRetiredWorkers() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(4);

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    async.onError(errors::add);

    // Worker 3 is one of the two the shrink below retires. Park it on a gate first, so the tasks queued behind the
    // gate are provably still IN ITS QUEUE - not already run - when the resize reaches it. The gate holds for
    // WORKER_HOLD_MS, comfortably longer than the one-second marker timeout the old teardown used before it
    // escalated to interrupt(): that escalation is the mechanism under test, and it must have had time to fire.
    final CountDownLatch gateEntered = new CountDownLatch(1);
    final AtomicBoolean gateInterrupted = new AtomicBoolean();
    assertThat(async.scheduleTask(3, new CountingTask(executed, completed) {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal db) {
        super.execute(asyncThread, db);
        gateEntered.countDown();
        try {
          Thread.sleep(WORKER_HOLD_MS);
        } catch (final InterruptedException e) {
          gateInterrupted.set(true);
          Thread.currentThread().interrupt();
        }
      }
    }, true, 0)).isTrue();
    assertThat(gateEntered.await(30, TimeUnit.SECONDS)).as("the gate task must be running on worker 3").isTrue();

    // FILL worker 3's queue to the brim, which is what makes the old teardown lossy: it offered its exit marker
    // with a one-second timeout, and a full queue refused it, so the worker was interrupt()ed - the gate task
    // unwound and everything behind it was drained straight into completed() without ever running. Filled through
    // the non-blocking form so the exact capacity (ASYNC_OPERATIONS_QUEUE_SIZE / parallelLevel) never has to be
    // hardcoded here.
    int queuedBehindTheGate = 0;
    while (async.scheduleTask(3, new CountingTask(executed, completed), false, 0))
      queuedBehindTheGate++;
    assertThat(queuedBehindTheGate).as("worker 3's queue must be full before the resize").isGreaterThan(0);

    // On its own thread because the resize now WAITS for the retired workers to finish draining.
    final AtomicReference<Throwable> resizeFailure = new AtomicReference<>();
    final Thread resizer = new Thread(() -> {
      try {
        async.setParallelLevel(2);
      } catch (final Throwable e) {
        resizeFailure.set(e);
      }
    }, "issue6526-resizer");
    resizer.start();

    resizer.join(60_000);
    assertThat(resizer.isAlive()).as("the resize must not hang").isFalse();
    assertThat(resizeFailure.get()).isNull();
    assertThat(gateInterrupted.get())
        .as("a resize must never interrupt a worker: that is what unwinds the task in flight").isFalse();

    assertThat(async.getThreadCount()).as("the pool must actually be smaller afterwards").isEqualTo(2);
    assertThat(executed.get())
        .as("every task queued on the retired worker must have RUN, not merely been notified as completed")
        .isEqualTo(queuedBehindTheGate + 1);
    assertThat(completed.get()).isEqualTo(queuedBehindTheGate + 1);
    assertThat(errors).as("no task may fail because the pool was resized under it").isEmpty();
  }

  /**
   * Item 1, the half that FAILED UNRELATED CALLERS. A producer scheduling throughout a stream of resizes must never
   * see {@code "Async executor has been shut down"}: that exception means the database is closing, and #6505 is what
   * happens when a best-effort scheduling hook believes it - an index-compaction hook running after
   * {@code writeTransactionToWAL()} crossed the point of no return and fenced the whole database.
   */
  @Test
  @Timeout(120)
  void resizingTheParallelLevelNeverTellsAnUnrelatedProducerTheExecutorIsShutDown() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(4);

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final AtomicInteger scheduled = new AtomicInteger();
    final List<Throwable> producerFailures = new CopyOnWriteArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean();

    final Thread producer = new Thread(() -> {
      while (!stop.get()) {
        try {
          // Slot -1 goes through getBestSlot(), which reads the published array and threw the moment
          // createThreads() had nulled it.
          if (async.scheduleTask(-1, new CountingTask(executed, completed), true, 0))
            scheduled.incrementAndGet();
        } catch (final Throwable e) {
          producerFailures.add(e);
        }
      }
    }, "issue6526-producer");
    producer.start();

    try {
      for (int i = 0; i < 20; i++)
        async.setParallelLevel(i % 2 == 0 ? 2 : 4);
    } finally {
      stop.set(true);
      producer.join(30_000);
    }
    assertThat(producer.isAlive()).isFalse();

    assertThat(producerFailures)
        .as("a parallel-level change must never look like a database close to an unrelated producer")
        .isEmpty();
    assertThat(scheduled.get()).as("the producer must have kept working throughout the resizes").isGreaterThan(0);

    assertThat(async.waitCompletion(60_000)).isTrue();
    assertThat(executed.get())
        .as("waitCompletion() must cover the workers still draining a shrink, so nothing is left unrun")
        .isEqualTo(scheduled.get());
  }

  /**
   * Item 1, the terminal path (#6526 review, finding 1). {@code kill()} read only the published array, so a worker
   * left draining by a shrink whose wait had expired was never force-shut-down, never interrupted and never joined:
   * {@code kill()} returned while a thread of this database kept running against a database it had just stopped.
   * The shrink here is made to outlive its own drain budget - {@code shutdownJoinTimeoutMs} is dropped to a few
   * milliseconds - which is precisely the case the design leaves running in the background.
   */
  @Test
  @Timeout(60)
  void killReachesTheWorkersAShrinkLeftDraining() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(4);

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();

    final CountDownLatch gateEntered = new CountDownLatch(1);
    assertThat(async.scheduleTask(3, new CountingTask(executed, completed) {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal ignored) {
        super.execute(asyncThread, ignored);
        gateEntered.countDown();
        try {
          Thread.sleep(UNTIL_INTERRUPTED_MS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, true, 0)).isTrue();
    assertThat(gateEntered.await(30, TimeUnit.SECONDS)).isTrue();

    // Make the shrink give up on the drain almost immediately, so worker 3 is provably still alive and still
    // registered as retiring when kill() runs.
    final long previousJoinTimeout = async.shutdownJoinTimeoutMs;
    async.shutdownJoinTimeoutMs = 10;
    final DatabaseAsyncExecutorImpl.AsyncThread retiredWorker = workerNamed(3);
    assertThat(retiredWorker).isNotNull();
    try {
      async.setParallelLevel(2);
      assertThat(retiredWorker.isAlive()).as("the drain budget must have expired with the worker still running")
          .isTrue();

      // #6526 review round 4: a worker abandoned past its drain budget has to be VISIBLE, not just logged once at
      // the moment the wait gave up. This is the only state in which the gauge is non-zero for longer than a
      // resize takes, and it is the state an operator needs to be able to see.
      assertThat(async.getStats().retiringWorkers)
          .as("a worker still draining past the resize budget must be reported").isEqualTo(1L);
      assertThat(Profiler.INSTANCE.toJSON().getJSONObject("asyncRetiringWorkers").getLong("value", -1L))
          .as("and must reach the profiler, as a gauge rather than a counter").isGreaterThanOrEqualTo(1L);

      async.kill();

      retiredWorker.join(10_000);
      assertThat(retiredWorker.isAlive()).as("kill() must not leave a retired worker of this database running")
          .isFalse();
      assertThat(async.getStats().retiringWorkers)
          .as("and the gauge must come back down once nothing is draining").isZero();
    } finally {
      async.shutdownJoinTimeoutMs = previousJoinTimeout;
    }
  }

  /**
   * Item 1, concurrent resizes (#6526 review, finding 2). The drain wait is what keeps two workers off one bucket's
   * pages while a shrink finishes, and it only means that if a second caller cannot grow the pool back through the
   * middle of it: a grow refills the array positions the shrink vacated, so slot 2 would name a fresh worker AND
   * the old slot-2 worker still running its pre-shrink tasks.
   */
  @Test
  @Tag("slow")
  @Timeout(60)
  void aConcurrentGrowCannotOvertakeAShrinkStillDraining() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(4);

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();

    final CountDownLatch gateEntered = new CountDownLatch(1);
    assertThat(async.scheduleTask(3, new CountingTask(executed, completed) {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal db) {
        super.execute(asyncThread, db);
        gateEntered.countDown();
        try {
          Thread.sleep(WORKER_HOLD_MS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, true, 0)).isTrue();
    assertThat(gateEntered.await(30, TimeUnit.SECONDS)).isTrue();

    final DatabaseAsyncExecutorImpl.AsyncThread retiredWorker = workerNamed(3);
    assertThat(retiredWorker).isNotNull();

    final Thread shrinker = new Thread(() -> async.setParallelLevel(2), "issue6526-shrinker");
    shrinker.start();

    // Spin until the shrink has published its smaller pool, so the grow below provably starts while the retired
    // worker is still draining rather than before the shrink got anywhere.
    // Sleeping rather than spinning: a tight onSpinWait() loop on this thread starves the shrinker of the CPU it
    // needs to get anywhere, which turned a three-second test into a fifty-second one on a loaded fork.
    final long deadline = System.currentTimeMillis() + 30_000;
    while (async.getThreadCount() != 2 && System.currentTimeMillis() < deadline)
      Thread.sleep(2);
    assertThat(async.getThreadCount()).as("the shrink must have published before the grow starts").isEqualTo(2);

    final AtomicBoolean oldWorkerStillAliveAtGrow = new AtomicBoolean();
    final Thread grower = new Thread(() -> {
      async.setParallelLevel(4);
      oldWorkerStillAliveAtGrow.set(retiredWorker.isAlive());
    }, "issue6526-grower");
    grower.start();

    grower.join(45_000);
    shrinker.join(45_000);
    assertThat(grower.isAlive()).isFalse();
    assertThat(shrinker.isAlive()).isFalse();

    assertThat(oldWorkerStillAliveAtGrow.get())
        .as("a grow must not republish a slot while the worker the shrink retired for it is still running")
        .isFalse();
    assertThat(async.getThreadCount()).isEqualTo(4);
  }

  /**
   * Item 1, the stale-slot fallback in {@code scheduleTask()} (#6526 review round 5), pinned directly rather than
   * left to be hit by chance. {@code getSlot()} maps a bucket modulo the pool size it read, and the pool can shrink
   * before the caller reaches {@code scheduleTask()} - a window that did not exist while a resize nulled the array
   * for its whole duration, because the caller got the "shut down" exception instead. The index is re-mapped rather
   * than thrown on, so the task still runs.
   * <p>
   * Deterministic, and does not need a race to be: an index obtained from an eight-worker pool is simply carried
   * across the shrink by hand, which is precisely the value the racing caller would be holding.
   */
  @Test
  @Timeout(60)
  void aSlotIndexLeftStaleByAShrinkStillReachesALiveWorker() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(8);

    final int staleSlot = async.getSlot(7);
    assertThat(staleSlot).as("the bucket must map into the upper half of the pool it was read against").isEqualTo(7);

    async.setParallelLevel(2);
    assertThat(staleSlot)
        .as("the slot must now be out of bounds for the published pool, or the fallback is not being exercised")
        .isGreaterThanOrEqualTo(async.getThreadCount());

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    async.onError(errors::add);

    assertThat(async.scheduleTask(staleSlot, new CountingTask(executed, completed), true, 0))
        .as("a stale slot must be re-mapped onto a live worker, not thrown on").isTrue();
    assertThat(async.waitCompletion(30_000)).isTrue();

    assertThat(executed.get()).as("and the task must actually run there").isEqualTo(1);
    assertThat(errors).isEmpty();
  }

  /**
   * Item 1, the boundary the drain wait actually holds (#6526 review round 6). A producer pinned to ONE bucket -
   * the case round-robin scheduling cannot reach, and the case where a shrink migrates that bucket from a retiring
   * worker to a survivor - must keep working across the resize: every task it schedules runs, and it is never
   * refused.
   * <p>
   * What this deliberately does NOT assert is that the two workers never touch the bucket's pages at the same time,
   * because they can: the moment the truncated array is published, {@code getSlot()} sends the producer to the
   * survivor while the retired worker is still draining that bucket's queued tasks. That overlap is the accepted
   * residual documented on {@link DatabaseAsyncExecutorImpl#setParallelLevel(int)} - bounded by the drain, costing
   * an MVCC conflict the async path already reports and retries - and a test claiming otherwise would be pinning a
   * guarantee the code does not make. What it pins instead is the guarantee it does: nothing is lost, nobody is
   * refused.
   */
  @Test
  @Timeout(60)
  void aProducerPinnedToOneBucketKeepsWorkingAcrossAShrink() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(4);

    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final AtomicInteger scheduled = new AtomicInteger();
    final List<Throwable> producerFailures = new CopyOnWriteArrayList<>();
    final List<Throwable> taskErrors = new CopyOnWriteArrayList<>();
    async.onError(taskErrors::add);

    // BUCKET 3 is the one that migrates: 3 % 4 = 3, a worker the shrink below retires, and 3 % 2 = 1, one that
    // survives. Routed through getSlot() on every iteration exactly as the engine's own createRecord() does, so the
    // producer follows the mapping across the resize instead of pinning to a slot number.
    final AtomicBoolean stop = new AtomicBoolean();
    final Thread producer = new Thread(() -> {
      while (!stop.get()) {
        try {
          if (async.scheduleTask(async.getSlot(3), new CountingTask(executed, completed), true, 0))
            scheduled.incrementAndGet();
        } catch (final Throwable e) {
          producerFailures.add(e);
        }
      }
    }, "issue6526-bucket-producer");
    producer.start();

    try {
      async.setParallelLevel(2);
      async.setParallelLevel(4);
      async.setParallelLevel(2);
    } finally {
      stop.set(true);
      producer.join(30_000);
    }
    assertThat(producer.isAlive()).isFalse();

    assertThat(producerFailures)
        .as("a producer pinned to a migrating bucket must never be refused by a resize").isEmpty();
    assertThat(scheduled.get()).as("the producer must have kept working throughout").isGreaterThan(0);

    assertThat(async.waitCompletion(60_000)).isTrue();
    assertThat(executed.get())
        .as("and every task it scheduled must have run, on whichever worker owned the bucket at the time")
        .isEqualTo(scheduled.get());
    assertThat(taskErrors).as("no task may fail because its bucket changed owner").isEmpty();
  }

  /**
   * Item 1 against the barrier (#6526 review round 8). {@code quiesceWorkers()} snapshots the pool once and then
   * schedules one park task per slot, but {@code scheduleTask()} re-reads the published array on every call - and
   * since this PR a slot the pool has outgrown is re-mapped onto a live worker rather than refused. A shrink
   * landing inside that loop therefore used to fold several park tasks onto one survivor, which runs the first,
   * counts the latch down once and blocks: the duplicates behind it never run, the latch never reaches zero, and
   * the caller - an index build - waits out the whole quiesce timeout before failing.
   * <p>
   * Driven from the worst end: the resize runs in a tight loop while the quiescence is repeatedly established and
   * released, so the two interleave many times rather than once by luck.
   */
  @Test
  @Tag("slow")
  @Timeout(120)
  void quiescingWhileTheParallelLevelChangesStillParksEveryWorker() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(5);

    final AtomicBoolean stop = new AtomicBoolean();
    final List<Throwable> resizeFailures = new CopyOnWriteArrayList<>();
    final Thread resizer = new Thread(() -> {
      for (int i = 0; !stop.get(); i++)
        try {
          async.setParallelLevel(i % 2 == 0 ? 2 : 5);
        } catch (final Throwable e) {
          resizeFailures.add(e);
        }
    }, "issue6526-quiesce-resizer");
    resizer.start();

    try {
      // Each quiescence must complete promptly. Before the fix a collapsed park loop could not reach its latch and
      // only unblocked when quiesceTimeoutMillis() (60s) expired, so a run that finishes at all finishes fast.
      for (int i = 0; i < 40; i++)
        async.quiesceWorkers().close();
    } finally {
      stop.set(true);
      resizer.join(30_000);
    }

    assertThat(resizer.isAlive()).isFalse();
    assertThat(resizeFailures).as("a resize must not fail because a quiescence was running").isEmpty();
  }

  /**
   * Item 1, growing. Raising the level must not disturb the workers that already exist: they own batch transactions
   * and are the unit {@code ThreadBucketSelectionStrategy} pins buckets to, so respawning them is never free.
   */
  @Test
  @Timeout(60)
  void raisingTheParallelLevelLeavesTheExistingWorkersRunning() {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(2);

    final Set<Long> before = asyncWorkerThreadIds();
    assertThat(before).hasSize(2);

    async.setParallelLevel(5);

    assertThat(async.getThreadCount()).isEqualTo(5);
    assertThat(asyncWorkerThreadIds()).as("growing must ADD workers, never replace the ones already running")
        .hasSize(5).containsAll(before);
  }

  /**
   * Item 2. A DDL command dispatched with {@code awaitResponse=false} runs on {@code AsyncCommandPool} since #6303,
   * in a transaction that runner opens itself - and that transaction must carry the executor's durability policy,
   * exactly as every task running on a worker does since #6511. Before this fix it carried whatever the ambient
   * default happened to be, so a caller that had set {@code setTransactionUseWAL(false)} around a bulk load found
   * the DDL it dispatched inside that window silently unaffected by it.
   */
  @Test
  @Timeout(120)
  void dispatchedDDLRunsUnderTheExecutorDurabilityPolicy() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();

    async.setTransactionUseWAL(false);
    async.setTransactionSync(WALFile.FlushType.NO);
    final Observation withoutWAL = dispatchDDLObservingItsTransaction(db, "Issue6526A");
    assertThat(withoutWAL.transactionActive).as("the dispatched DDL must run inside the runner's own transaction")
        .isTrue();
    assertThat(withoutWAL.useWAL).as("the executor's transactionUseWAL must reach the dispatched DDL").isFalse();
    assertThat(withoutWAL.walFlush).isEqualTo(WALFile.FlushType.NO);

    async.setTransactionUseWAL(true);
    async.setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
    final Observation withWAL = dispatchDDLObservingItsTransaction(db, "Issue6526B");
    assertThat(withWAL.useWAL).as("and so must the flip back, on the very next dispatched command").isTrue();
    assertThat(withWAL.walFlush).isEqualTo(WALFile.FlushType.YES_NOMETADATA);

    assertThat(database.getSchema().existsType("Issue6526A")).isTrue();
    assertThat(database.getSchema().existsType("Issue6526B")).isTrue();

    // The submitting thread's own transaction settings must be untouched by any of it.
    database.transaction(() -> assertThat(db.getTransaction().isUseWAL())
        .as("dispatching a command must not restamp the submitter's own transaction").isTrue());
  }

  /**
   * Item 3. {@code closeTransactionBoundaryIfDurabilityPolicyChanged()} is what makes a flag flip take effect
   * without the pool teardown, and it does so by cutting the open batch short. That is the accepted cost of the
   * #6511 trade, and until now nothing counted it: the incident behind #6509 was reconstructed by grepping
   * {@code InterruptedIOException}s out of seven hours of logs.
   */
  @Test
  @Timeout(60)
  void forcedBoundaryCommitsAreCounted() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(1);
    async.setCommitEvery(1000); // ONE LONG BATCH, SO ONLY A FLAG FLIP CAN CLOSE IT

    async.setTransactionUseWAL(true);
    async.setTransactionSync(WALFile.FlushType.NO);
    runOneTaskInATransaction(async);

    final long baseline = async.getStats().forcedBoundaryCommits;

    async.setTransactionUseWAL(false);
    runOneTaskInATransaction(async);
    assertThat(async.getStats().forcedBoundaryCommits - baseline)
        .as("the first task after a flip must close the batch stamped with the old policy").isEqualTo(1L);

    runOneTaskInATransaction(async);
    assertThat(async.getStats().forcedBoundaryCommits - baseline)
        .as("a task that changes nothing must not force anything").isEqualTo(1L);

    async.setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
    runOneTaskInATransaction(async);
    assertThat(async.getStats().forcedBoundaryCommits - baseline)
        .as("the sync flag counts too, not just useWAL").isEqualTo(2L);
  }

  /**
   * Item 3, the part that must cost nothing (#6526 review round 1, found by the regression it caused). The counter
   * is folded into {@code Profiler} on every metrics scrape AND on every database close, and reaching it through
   * {@code db.async()} rather than the field made merely OBSERVING a database grow it a full set of worker threads.
   * On the close path that is a leak with no owner: the close has already shut down whatever executor existed, so
   * the pool created behind it has nothing left to stop it. {@code waitForAsyncCompletion()} and
   * {@code quiesceAsync()} both carry the same rule in their comments; this pins it for the profiler too.
   */
  @Test
  @Timeout(60)
  void readingTheProfilerCountersNeverCreatesAnAsyncExecutor() throws Exception {
    final String dbName = "Issue6526NoAsyncProfiler";
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + dbName);
    if (factory.exists())
      factory.open().drop();

    final LocalDatabase db = (LocalDatabase) factory.create();
    try {
      // A database that has never touched the async API. Nothing below may change that.
      assertThat(db.getAsyncIfExists()).as("a fresh database must have no asynchronous executor").isNull();

      Profiler.INSTANCE.toJSON();
      assertThat(db.getAsyncIfExists())
          .as("a metrics scrape must not start a worker pool on a database that never used async").isNull();
    } finally {
      db.drop();
    }

    // The close path folds the same counters in through unregisterDatabase(), where a pool created behind the close
    // would never be shut down at all: no worker thread of this database may be alive now.
    final String prefix = "AsyncExecutor-" + dbName + "-";
    for (final Thread t : Thread.getAllStackTraces().keySet())
      assertThat(t.getName().startsWith(prefix) && t.isAlive())
          .as("closing a database must not leave (or start) async worker threads: %s", t.getName()).isFalse();
  }

  private record Observation(boolean transactionActive, boolean useWAL, WALFile.FlushType walFlush) {
  }

  /**
   * Dispatches a script whose DDL half sends it to {@link AsyncCommandPool} and reads the durability flags of the
   * transaction it is running in, from inside the command itself: {@code userCallback.onComplete} is invoked by
   * {@code DatabaseAsyncCommand.execute()}, which the runner calls between its own {@code begin()} and
   * {@code commit()}.
   */
  private Observation dispatchDDLObservingItsTransaction(final DatabaseInternal db, final String typeName)
      throws Exception {
    final AtomicReference<Observation> observation = new AtomicReference<>();
    final AtomicReference<Exception> failure = new AtomicReference<>();
    final AtomicReference<String> ranOn = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);

    db.async().command("sqlscript", "CREATE VERTEX TYPE " + typeName + "; INSERT INTO " + typeName + " SET id = 1;",
        new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
            ranOn.set(Thread.currentThread().getName());
            observation.set(db.isTransactionActive() ?
                new Observation(true, db.getTransaction().isUseWAL(), db.getTransaction().getWALFlush()) :
                new Observation(false, false, null));
            done.countDown();
          }

          @Override
          public void onError(final Exception exception) {
            failure.set(exception);
            done.countDown();
          }
        });

    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
    assertThat(failure.get()).isNull();
    assertThat(ranOn.get()).as("a script carrying DDL must be dispatched to the async command pool")
        .startsWith("ArcadeDB-AsyncCommand-");
    db.async().waitCompletion();
    return observation.get();
  }

  private static void runOneTaskInATransaction(final DatabaseAsyncExecutorImpl async) throws Exception {
    final CountDownLatch done = new CountDownLatch(1);
    assertThat(async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        // The task only has to exist: what is under test is the boundary check that runs BEFORE it.
      }

      @Override
      public void completed() {
        done.countDown();
      }
    }, true, 0)).isTrue();
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
  }

  /** The live worker of this database occupying the given slot, by the name {@code AsyncThread} gives itself. */
  private DatabaseAsyncExecutorImpl.AsyncThread workerNamed(final int slot) {
    final String name = "AsyncExecutor-" + database.getName() + "-" + slot;
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t instanceof final DatabaseAsyncExecutorImpl.AsyncThread worker && name.equals(t.getName()) && t.isAlive())
        return worker;
    return null;
  }

  private Set<Long> asyncWorkerThreadIds() {
    final String prefix = "AsyncExecutor-" + database.getName() + "-";
    final Set<Long> ids = new HashSet<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.getName().startsWith(prefix) && t.isAlive())
        ids.add(t.threadId());
    return ids;
  }

  /** Counts executions and completions separately: a dropped task is completed WITHOUT ever being executed. */
  private static class CountingTask implements DatabaseAsyncTask {
    private final AtomicInteger executed;
    private final AtomicInteger completed;

    private CountingTask(final AtomicInteger executed, final AtomicInteger completed) {
      this.executed = executed;
      this.completed = completed;
    }

    @Override
    public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
      executed.incrementAndGet();
    }

    @Override
    public void completed() {
      completed.incrementAndGet();
    }

    @Override
    public boolean requiresActiveTx() {
      return false;
    }
  }
}
