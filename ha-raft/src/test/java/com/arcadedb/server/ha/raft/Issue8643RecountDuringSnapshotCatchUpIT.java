/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster-level regression test for issue #8640, tracked by #8643.
 * <p>
 * After a leader snapshot install every bucket counter on the follower is unknown (-1): the archive ships no
 * {@code statistics.json}. The first {@code count(*)} recomputes it with a page scan, and before #8640 the catch-up
 * entries the follower applied meanwhile took no bucket lock, so the scan missed or double-counted them and then
 * published the result. The stored counter stayed wrong for good, while a scan of the same type was right.
 * <p>
 * The engine-level {@code Issue8640ApplyChangesRecountRaceTest} stands in for the recompute with a hand-held lock and
 * drives {@code TransactionManager.applyChanges} directly. This test runs the real thing on a 3-node cluster:
 * <ol>
 *   <li>a leader under steady write load,</li>
 *   <li>a follower kept down until the leader compacted past it, so it rejoins through a snapshot install,</li>
 *   <li>a reader running {@code SELECT count(*)} on that follower while its catch-up is still applying entries,</li>
 *   <li>and, once everything converged, {@code count(*)} (the stored counter) compared with a scan on every node.</li>
 * </ol>
 * The overlap is timing-dependent, so the test repeats the stop/install cycle until the reader has seen a recount that
 * started on an unknown counter while the follower's applied index moved, and asserts it did: a run that never raced
 * would otherwise pass without testing anything.
 */
@Tag("slow")
class Issue8643RecountDuringSnapshotCatchUpIT extends BaseRaftHATest {

  private static final String TYPE_NAME       = "Issue8643";
  private static final int    INITIAL_RECORDS = 100_000;
  private static final int    MAX_ROUNDS      = 4;
  // Small pages: the recount reads every page of the bucket one by one, so thousands of them keep it scanning long
  // enough for the follower to apply entries meanwhile
  private static final int    PAGE_SIZE       = 16_384;
  // The leader purges its log once every PURGE_GAP entries; the follower stays down for more than that, so it always
  // finds the leader's log compacted past it
  private static final int    PURGE_GAP       = 1_000;
  private static final int    OFFLINE_TXS     = 3 * PURGE_GAP / 2;
  // Makes each entry a few KB, so the offline writes fill and close several log segments that the purge can then drop
  private static final String PAYLOAD         = "x".repeat(1_024);
  // Gives the initial records some bulk, so a recount scans enough pages to span several applied entries
  private static final String SMALL_PAYLOAD   = "y".repeat(200);
  // Load kept on the leader after the follower is back, so the catch-up still applies entries when the reader recounts
  private static final long   ONLINE_LOAD_MS  = 6_000;

  private final AtomicLong nextId       = new AtomicLong();
  private final AtomicLong nextDeleteId = new AtomicLong();

  @Override
  protected int getServerCount() {
    // 3 nodes so a majority (2) keeps the cluster writable while the follower is down
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Snapshot every few entries and purge in small log segments (only a closed segment is purged): a follower down for
    // OFFLINE_TXS entries then finds the leader's log compacted past it, and can only rejoin by a snapshot install. The
    // purge runs once every PURGE_GAP entries rather than after every snapshot, so the entries written while the
    // install runs are still in the leader's log afterwards and the follower catches up on them by replay, instead of
    // falling behind the next purge and installing again in a loop
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD, 10L);
    config.setValue(GlobalConfiguration.HA_LOG_PURGE_GAP, PURGE_GAP);
    config.setValue(GlobalConfiguration.HA_LOG_PURGE_UPTO_SNAPSHOT, true);
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "256KB");
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "512KB");
    config.setValue(GlobalConfiguration.HA_LOG_SEGMENT_SIZE, "512KB");
  }

  @Test
  @Timeout(900)
  void storedCountMatchesScanAfterRecountOverlapsSnapshotCatchUp() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.transaction(() -> {
      // One bucket: every applied entry then lands in the bucket the recount is scanning, rather than in one it already
      // published or has not reached yet, both of which the apply handles correctly even without the #8640 lock
      leaderDb.getSchema().createDocumentType(TYPE_NAME, 1, PAGE_SIZE).createProperty("id", Long.class);
      leaderDb.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "id");
    });
    // A type big enough that a recount scans for a while, so it has room to overlap the entries applied meanwhile
    for (int batch = 0; batch < INITIAL_RECORDS / 500; batch++)
      leaderDb.transaction(() -> {
        for (int i = 0; i < 500; i++)
          leaderDb.newDocument(TYPE_NAME).set("id", nextId.getAndIncrement()).set("payload", SMALL_PAYLOAD).save();
      });
    assertClusterConsistency();

    int installs = 0;
    int recountsOnUnknownCounter = 0;
    int overlappingRecounts = 0;
    for (int round = 1; round <= MAX_ROUNDS && overlappingRecounts == 0; round++) {
      final RoundResult result = runRound(round, leaderIndex, followerIndex);
      if (result.installed)
        installs++;
      recountsOnUnknownCounter += result.recountsOnUnknownCounter;
      overlappingRecounts += result.overlappingRecounts;
      LogManager.instance().log(this, Level.INFO,
          "TEST: round %d: install=%s, recounts on an unknown counter=%d, of which overlapping the catch-up=%d", round,
          result.installed, result.recountsOnUnknownCounter, result.overlappingRecounts);
    }

    if (overlappingRecounts == 0)
      LogManager.instance().log(this, Level.WARNING,
          "TEST: no recount overlapped the catch-up in %d rounds: installs=%d, recounts on an unknown counter=%d", MAX_ROUNDS,
          installs, recountsOnUnknownCounter);
    // The scenario must actually have happened, or the final comparison proves nothing
    assertThat(installs).as("the follower must rejoin through a leader snapshot install at least once").isGreaterThan(0);
    assertThat(recountsOnUnknownCounter).as("count(*) must have recounted an unknown counter on the follower").isGreaterThan(0);
    assertThat(overlappingRecounts).as("a recount on the follower must have overlapped its catch-up applies").isGreaterThan(0);

    assertClusterConsistency();

    final long expected = scanCount(leaderIndex);
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final long stored = withResyncRetry(serverIndex, db -> storedCount(db));
      final long scanned = scanCount(serverIndex);
      assertThat(scanned).as("scan count on server %d must match the leader's", serverIndex).isEqualTo(expected);
      assertThat(stored).as("stored count(*) on server %d must match a scan of the same type (issue #8640)", serverIndex)
          .isEqualTo(scanned);
    }
  }

  private record RoundResult(boolean installed, int recountsOnUnknownCounter, int overlappingRecounts) {
  }

  private RoundResult runRound(final int round, final int leaderIndex, final int followerIndex) throws InterruptedException {
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    LogManager.instance().log(this, Level.INFO, "TEST: round %d: stopping follower %d", round, followerIndex);
    getServer(followerIndex).stop();

    for (int tx = 0; tx < OFFLINE_TXS; tx++)
      insertTransaction(leaderDb);

    final AtomicBoolean loadRunning = new AtomicBoolean(true);
    final AtomicReference<Throwable> loadError = new AtomicReference<>();
    final Thread load = new Thread(() -> {
      try {
        while (loadRunning.get())
          deleteTransaction(leaderDb);
      } catch (final Throwable t) {
        loadError.set(t);
      }
    }, "issue8643-load");

    final AtomicBoolean readerRunning = new AtomicBoolean(true);
    final AtomicInteger recountsOnUnknownCounter = new AtomicInteger();
    final AtomicInteger overlappingRecounts = new AtomicInteger();
    final AtomicReference<Exception> readerLastError = new AtomicReference<>();
    final Thread reader = new Thread(
        () -> recountWhileCatchingUp(leaderIndex, followerIndex, readerRunning, recountsOnUnknownCounter, overlappingRecounts,
            readerLastError),
        "issue8643-follower-reader");

    load.start();
    reader.start();
    try {
      LogManager.instance().log(this, Level.INFO, "TEST: round %d: restarting follower %d under load", round, followerIndex);
      restartServer(followerIndex);
      // restartServer() returns once the follower reached the leader's applied index, which is usually before the reader
      // has seen it apply past the copied prefix: the load and the reader keep going so that window still comes
      Thread.sleep(ONLINE_LOAD_MS);
    } finally {
      loadRunning.set(false);
      load.join(60_000);
      readerRunning.set(false);
      reader.join(60_000);
    }
    assertThat(loadError.get()).as("the leader load must not fail").isNull();

    if (overlappingRecounts.get() == 0 && readerLastError.get() != null)
      LogManager.instance().log(this, Level.WARNING, "TEST: round %d: last error the follower reader skipped", readerLastError.get(),
          round);

    final ArcadeStateMachine stateMachine = stateMachineOrNull(followerIndex);
    final boolean installed = stateMachine != null && stateMachine.isBelowInstalledRaftBoundary(0);
    return new RoundResult(installed, recountsOnUnknownCounter.get(), overlappingRecounts.get());
  }

  /**
   * Written while the follower is down: inserts only, with a payload that fills the leader's log segments so the purge
   * can drop them.
   */
  private void insertTransaction(final Database leaderDb) {
    leaderDb.transaction(() -> {
      for (int i = 0; i < 5; i++)
        leaderDb.newDocument(TYPE_NAME).set("id", nextId.getAndIncrement()).set("payload", PAYLOAD).save();
    });
  }

  /**
   * Written while the follower catches up: deletes of the oldest live records, which is what a recount that overlaps
   * the apply gets wrong without the #8640 lock. Each record sits on one of the first pages of the bucket, which the
   * scan has already counted by the time the apply removes it, while the apply, seeing an unknown counter, folds
   * nothing: the published counter keeps every such record. They go through the unique index, so the load stays fast
   * enough to keep the follower's catch-up applying entries while it recounts.
   */
  private void deleteTransaction(final Database leaderDb) {
    leaderDb.transaction(() -> {
      for (int i = 0; i < 3; i++)
        leaderDb.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE id = ?", nextDeleteId.getAndIncrement());
    });
  }

  /**
   * Runs {@code count(*)} on the follower for the whole round, but only in the window the issue is about: after a
   * leader snapshot install, once the follower applies entries the installed copy did not already hold, with every
   * counter still unknown. A {@code count(*)} issued any earlier - between the swap of the installed copy and the first
   * applied entry, or while the catch-up only replays entries whose pages came with the copy - publishes a clean
   * counter and leaves nothing to race with. A call that starts while any bucket of the type has an unknown counter is a
   * recount; when the follower's applied index advanced during that call, the recount overlapped catch-up applies.
   * Failures are skipped, the last one kept for the round's log: the follower is down, opening, or swapping in the
   * installed copy for part of the round.
   */
  private void recountWhileCatchingUp(final int leaderIndex, final int followerIndex, final AtomicBoolean running,
      final AtomicInteger recounts, final AtomicInteger overlapping, final AtomicReference<Exception> lastError) {
    long copiedPrefixEnd = -1;
    while (running.get()) {
      try {
        final ArcadeDBServer server = getServer(followerIndex);
        if (server == null || !server.isStarted() || !server.existsDatabase(getDatabaseName())) {
          Thread.sleep(5);
          continue;
        }
        final ArcadeStateMachine stateMachine = stateMachineOrNull(followerIndex);
        // No install yet (the boundary is per state machine, so a restart resets it), or the catch-up that follows the
        // install has not applied its first entry: not the window yet
        if (stateMachine == null || !stateMachine.isBelowInstalledRaftBoundary(0)
            || stateMachine.isBelowInstalledRaftBoundary(appliedIndex(followerIndex))) {
          Thread.sleep(1);
          continue;
        }
        // The installed copy is taken from the leader's live files, so it already holds the pages of some entries past
        // its snapshot index: replaying those advances no page version, folds nothing and cannot race. Wait until the
        // follower is past everything the leader had applied when the catch-up started, so the entries applied during
        // the recount are ones the copy did not have
        // A second install inside the round (its boundary at or past the latch) has a copied prefix of its own
        if (copiedPrefixEnd < 0 || stateMachine.isBelowInstalledRaftBoundary(copiedPrefixEnd))
          copiedPrefixEnd = appliedIndex(leaderIndex);
        if (appliedIndex(followerIndex) <= copiedPrefixEnd) {
          Thread.sleep(1);
          continue;
        }
        final Database db = server.getDatabase(getDatabaseName());
        if (!db.getSchema().existsType(TYPE_NAME)) {
          Thread.sleep(5);
          continue;
        }
        boolean unknown = false;
        for (final Bucket bucket : db.getSchema().getType(TYPE_NAME).getBuckets(false))
          if (bucket instanceof LocalBucket local && local.getCachedRecordCount() < 0) {
            unknown = true;
            break;
          }

        final long appliedBefore = appliedIndex(followerIndex);
        final long startNs = System.nanoTime();
        // On the embedded instance: the replicated wrapper's read-consistency barrier could hold the query until the
        // catch-up has applied, and the scan would then run after the applies instead of during them
        final long count = storedCount(((DatabaseInternal) db).getEmbedded());
        final long appliedAfter = appliedIndex(followerIndex);

        if (unknown) {
          recounts.incrementAndGet();
          LogManager.instance().log(this, Level.INFO,
              "TEST: recount on an unknown counter answered %d in %d ms, applied index %d -> %d", count,
              (System.nanoTime() - startNs) / 1_000_000, appliedBefore, appliedAfter);
          if (appliedBefore >= 0 && appliedAfter > appliedBefore)
            overlapping.incrementAndGet();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (final Exception e) {
        // Down, opening, or being replaced by the installed copy: try again
        lastError.set(e);
        try {
          Thread.sleep(5);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private ArcadeStateMachine stateMachineOrNull(final int serverIndex) {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    return plugin == null || plugin.getRaftHAServer() == null ? null : plugin.getRaftHAServer().getStateMachine();
  }

  private long appliedIndex(final int serverIndex) {
    final ArcadeStateMachine stateMachine = stateMachineOrNull(serverIndex);
    final TermIndex termIndex = stateMachine == null ? null : stateMachine.getLastAppliedTermIndex();
    return termIndex == null ? -1 : termIndex.getIndex();
  }

  /** {@code count(*)} over the type, answered from the stored per-bucket counters (recomputed when unknown). */
  private static long storedCount(final Database db) {
    return ((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME).next().getProperty("cnt")).longValue();
  }

  /** The same count by a scan of the records, which never reads the stored counters. */
  private long scanCount(final int serverIndex) {
    return withResyncRetry(serverIndex,
        db -> ((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME + " WHERE id IS NOT NULL").next()
            .getProperty("cnt")).longValue());
  }
}
