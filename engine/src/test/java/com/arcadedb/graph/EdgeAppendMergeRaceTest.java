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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adversarial race between the commutative edge-append merge (GRAPH_EDGE_APPEND_MERGE, new in 26.7.2) and the
 * structural writes it must never rebase over: edge REMOVAL (chunk relink + empty-chunk delete) on the classic
 * layout, with dedicated adder / remover / reader threads all hitting one hot vertex at once.
 * <p>
 * If a structural write ever failed to poison its page, a concurrent rebase would re-derive that page from
 * committed-state + appends and drop the relink, leaving a chain whose bytes no longer parse - the truncated
 * read surfaces as java.nio.BufferUnderflowException on the next traversal (the shape reported in #565).
 * <p>
 * #5570: what this test does NOT prove is that every transaction eventually commits. Under this much contention
 * on one page a transaction can lose the race for all of its attempts and give up with a
 * {@link com.arcadedb.exception.ConcurrentModificationException}, which extends {@link NeedRetryException} and is
 * a documented outcome applications must handle. Those are counted and bounded, not fatal; anything that is not
 * retryable - a reader whose traversal stopped parsing above all - still fails the test immediately.
 */
@Tag("slow")
class EdgeAppendMergeRaceTest extends TestHelper {
  private static final int ADDERS = 8, REMOVERS = 4, READERS = 4;

  private int     savedThreshold;
  private boolean savedMerge;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedMerge = GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(savedMerge);
  }

  @Test
  void addersRemoversAndReadersOnOneHotVertex() throws Exception {
    final RaceOutcome outcome = runRace(1500, 100);

    assertThat(outcome.merges()).as("edge-append rebase must actually have fired").isGreaterThan(0);

    // #5570: a handful of give-ups is the expected outcome of a fair race once the runner is slow enough, and
    // the version gaps observed in CI were 2 to 4, i.e. the loser was never stale. What would NOT be expected
    // is a large share of the workload never getting through: that is starvation or a livelock, and it shows up
    // here as this count jumping rather than as an intermittent red build.
    final int writeTx = outcome.writeTransactions();
    assertThat(outcome.writerGiveUps())
        .as("write transactions that exhausted their retries: %d of %d", outcome.writerGiveUps(), writeTx)
        .isLessThan(Math.max(16, writeTx / 20));

    // Readers are the corruption detector, so their give-ups need a floor rather than the writers' ceiling: a
    // run where readers mostly failed to complete a traversal would go green with the BufferUnderflowException
    // guard barely exercised. Assert the detector actually ran, in absolute terms and as a share.
    //
    // Neither bound re-couples the test to runner speed, which is the thing #5570 set out to remove:
    //
    // - a read-only transaction cannot give up at all. commit1stPhase locks exactly the files that
    //   lockFilesFromChanges() collects from modifiedPages / newPages / indexChanges, all empty here, so
    //   tryLockFiles iterates an empty list and no COMMIT_LOCK_TIMEOUT can elapse; with no modified page there
    //   is likewise nothing for checkPageVersion to reject. So readerGiveUps is structurally 0, which makes the
    //   share bound vacuous (n > n/2) until readers really do start failing - exactly the degradation it guards.
    // - the absolute floor is 4 traversals for the whole run, against 18000 write transactions the readers loop
    //   underneath, and the earliest ones parse a nearly empty chain. No runner is slow enough for that.
    final int readTx = outcome.readTransactions();
    assertThat(outcome.readerCommits())
        .as("full chain traversals completed: %d of %d reader transactions", outcome.readerCommits(), readTx)
        .isGreaterThanOrEqualTo(READERS)
        .isGreaterThan(readTx / 2);
  }

  /**
   * Same race with a single attempt per transaction, so retry exhaustion is the norm instead of the exception.
   * Proves the two halves of the contract in one shot: give-ups are tolerated and counted, and the final
   * consistency checks stay exact regardless of how many transactions gave up.
   */
  @Test
  void retryExhaustionIsToleratedAndCounted() throws Exception {
    final RaceOutcome outcome = runRace(200, 1);

    assertThat(outcome.writerGiveUps()).as("one attempt per transaction must produce give-ups on this workload")
        .isGreaterThan(0);
  }

  private RaceOutcome runRace(final int perAdder, final int attempts) throws Exception {
    // 26.7.2 SHAPE: no striped layout, so every writer contends on the hot vertex's single head chunk.
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(true);

    database.transaction(() -> {
      database.getSchema().createVertexType("Account", 4).createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TRANSFERS", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account");
      hub.set("number", 0);
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });
    final RID hubRID = hubHolder[0];

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final ConcurrentLinkedQueue<RID> live = new ConcurrentLinkedQueue<>();
    final AtomicInteger addedCount = new AtomicInteger();
    final AtomicInteger removedCount = new AtomicInteger();
    final AtomicInteger readerCommits = new AtomicInteger();
    // Tallied per role, not per writer/reader: an adder losing the append race and a remover losing the
    // structural race are different symptoms, and a starvation regression is worth attributing to one of them.
    final GiveUps adderGiveUps = new GiveUps();
    final GiveUps removerGiveUps = new GiveUps();
    final GiveUps readerGiveUps = new GiveUps();
    final AtomicBoolean addersDone = new AtomicBoolean();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < ADDERS; t++) {
      final int id = t;
      threads.add(new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perAdder; i++) {
            final int n = id * perAdder + i;
            final RID[] holder = new RID[1];
            if (!commit(() -> {
              final MutableVertex src = database.newVertex("Account");
              src.set("number", n + 1);
              src.save();
              src.newEdge("TRANSFERS", hubRID);
              holder[0] = src.getIdentity();
            }, attempts, adderGiveUps))
              // GAVE UP: nothing was committed, so nothing to count and nothing to offer to the removers.
              continue;

            // Only half are ever eligible for removal: the chain must stay long (many chunks) while the
            // removers churn it, instead of draining to empty.
            if ((n & 1) == 0)
              live.add(holder[0]);
            addedCount.incrementAndGet();
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "adder-" + t));
    }

    // REMOVERS: delete source vertices, which relinks (and may delete) chunks in the hub's chain while the
    // adders are appending to it - the structural-vs-append race the merge must never smooth over.
    for (int t = 0; t < REMOVERS; t++) {
      threads.add(new Thread(() -> {
        try {
          start.await();
          while (!addersDone.get() || !live.isEmpty()) {
            final RID victim = live.poll();
            if (victim == null) {
              Thread.yield();
              continue;
            }
            // A remover that gives up leaves its victim un-deleted and does not count it, so the expected edge
            // count stays exact. The victim is deliberately not re-queued: retrying it forever would hide the
            // give-up instead of reporting it.
            if (commit(() -> victim.asVertex(true).delete(), attempts, removerGiveUps))
              removedCount.incrementAndGet();
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "remover-" + t));
    }

    // READERS: traverse the hot vertex continuously; a chain whose bytes stopped parsing shows up here.
    for (int t = 0; t < READERS; t++) {
      threads.add(new Thread(() -> {
        try {
          start.await();
          while (!addersDone.get()) {
            if (commit(() -> {
              final Vertex hub = hubRID.asVertex(true);
              hub.countEdges(Vertex.DIRECTION.IN, "TRANSFERS");
              for (final Vertex ignored : hub.getVertices(Vertex.DIRECTION.IN, "TRANSFERS")) {
                // full traversal: forces every chunk in the chain to be parsed
              }
            }, attempts, readerGiveUps))
              // A completed traversal is one full parse of the chain, i.e. one actual exercise of the
              // corruption detector. Counted so the test can prove the detector really ran.
              readerCommits.incrementAndGet();
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "reader-" + t));
    }

    threads.forEach(Thread::start);
    start.countDown();
    for (int i = 0; i < ADDERS; i++)
      threads.get(i).join();
    addersDone.set(true);
    for (final Thread thread : threads)
      thread.join();

    final RaceOutcome outcome = new RaceOutcome(addedCount.get(), removedCount.get(), adderGiveUps.count.get(),
        removerGiveUps.count.get(), readerCommits.get(), readerGiveUps.count.get(),
        ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges);

    // The suite pins com.arcadedb to SEVERE, so a run with give-ups is the only one that prints: a genuine
    // starvation regression then shows up as this count jumping in the build log, well before it grows enough
    // to trip the assertions below.
    LogManager.instance()
        .log(this, outcome.writerGiveUps() + outcome.readerGiveUps() > 0 ? Level.SEVERE : Level.WARNING,
            "#5570 race outcome: added=%d removed=%d merges=%d traversals=%d give-ups: adder=%d remover=%d reader=%d%s%s%s",
            outcome.added(), outcome.removed(), outcome.merges(), outcome.readerCommits(), outcome.adderGiveUps(),
            outcome.removerGiveUps(), outcome.readerGiveUps(), adderGiveUps.describeFirst("adder"),
            removerGiveUps.describeFirst("remover"), readerGiveUps.describeFirst("reader"));

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed with a non-retryable error, first: " + errors.getFirst(),
          errors.getFirst());

    // addedCount / removedCount only move after a successful commit, so this stays exact no matter how many
    // transactions gave up.
    final int expected = outcome.added() - outcome.removed();
    database.transaction(() -> {
      assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "TRANSFERS")).isEqualTo(expected);
      int seen = 0;
      for (final Vertex ignored : hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS"))
        seen++;
      assertThat(seen).as("every surviving edge still reachable").isEqualTo(expected);
    });

    return outcome;
  }

  /**
   * Runs a transaction and reports whether it committed. A transaction that exhausted its attempts raises a
   * {@link NeedRetryException}: that is contention, not corruption, so it is counted and swallowed. Everything
   * else - a {@code BufferUnderflowException} from a reader above all - propagates and fails the test.
   * <p>
   * Contention give-ups are the ONLY tolerated outcome, deliberately. {@code transaction()} also retries
   * {@link com.arcadedb.exception.DuplicatedKeyException}, which does not extend {@link NeedRetryException} and
   * so would still fail this test on exhaustion - correct here, since this workload has no unique index and a
   * duplicate would mean the engine handed out a colliding RID. Anyone reusing this wrapper on a unique-key
   * workload has to decide that case explicitly rather than inherit this one.
   */
  private boolean commit(final BasicDatabase.TransactionScope block, final int attempts, final GiveUps giveUps) {
    try {
      database.transaction(block, true, attempts);
      return true;
    } catch (final NeedRetryException e) {
      giveUps.record(e);
      return false;
    }
  }

  /**
   * Per-role tally of transactions that ran out of attempts. Kept per role so the logged sample cannot be a
   * reader exception in a writer-dominated run.
   */
  private static final class GiveUps {
    final AtomicInteger              count = new AtomicInteger();
    final AtomicReference<Throwable> first = new AtomicReference<>();

    void record(final NeedRetryException e) {
      count.incrementAndGet();
      first.compareAndSet(null, e);
    }

    String describeFirst(final String role) {
      final Throwable e = first.get();
      return e != null ? " first(" + role + ")=" + e : "";
    }
  }

  private record RaceOutcome(int added, int removed, int adderGiveUps, int removerGiveUps, int readerCommits,
                            int readerGiveUps, long merges) {
    int writerGiveUps() {
      return adderGiveUps + removerGiveUps;
    }

    int writeTransactions() {
      return added + removed + writerGiveUps();
    }

    int readTransactions() {
      return readerCommits + readerGiveUps;
    }
  }
}
