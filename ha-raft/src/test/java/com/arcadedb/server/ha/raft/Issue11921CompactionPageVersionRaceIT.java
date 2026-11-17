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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.engine.timeseries.TimeSeriesShard;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11921: a TimeSeries compaction commits twice on the leader (phase 0 sets the compaction flag
 * on the shard's header page, phase 4c clears it) and publishes each commit locally before its {@code SCHEMA_ENTRY}
 * carries the WAL. A follower's append validated by the leader in that window could take the same version of the header
 * page: the leader's ledger read the local copy before the publication and trusted that read, since only an apply told
 * it the copy had moved. Followers then spliced the two changes, and the compaction's clear and sealed image removed an
 * acknowledged sample on every node.
 * <p>
 * The test opens the window deterministically: it holds the leader's validation monitor of the database, so the
 * follower's append stops right after the leader read the local version, runs the compaction's phase 0 meanwhile, then
 * lets the validation go on. Whatever the outcome, no two entries of the log may claim the same version of a page.
 */
class Issue11921CompactionPageVersionRaceIT extends BaseRaftHATest {
  private static final String TYPE = "Issue11921Ts";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void cleanupHooks() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
  }

  @Test
  void aFollowerAppendCannotTakeThePageVersionACompactionPublishes() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    leaderDb.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (v LONG) SHARDS 1");
    // The maintenance scheduler compacts the type 5 s after it is created: it would empty the mutable bucket before the
    // compaction below, which then returns at Phase 0 with nothing to publish and the race is never run
    leaderDb.getSchema().getEmbedded().getTimeSeriesMaintenanceScheduler().cancel(TYPE);
    waitForAllServers();
    for (int i = 0; i < 20; i++)
      leaderDb.command("sql", "INSERT INTO " + TYPE + " SET ts = ?, v = ?", 1_000L + i, (long) i);
    // One append from the follower, so the leader holds a ledger for the database
    append(followerDb, 2_000L);
    waitForAllServers();

    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final ReentrantLock validationLock = validationLock(leaderRaft.getStateMachine());
    final long logBefore = lastRaftLogIndex(leaderRaft);

    final int[] mutablePages = new int[1];
    leaderDb.transaction(() -> {
      try {
        mutablePages[0] = ((LocalTimeSeriesType) leaderDb.getSchema().getType(TYPE)).getEngine().getShard(0).getMutableBucket()
            .getDataPageCount();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
    assertThat(mutablePages[0]).as("the compaction must have samples to publish, or it never reaches its version check").isPositive();

    final CountDownLatch phase0Published = new CountDownLatch(1);
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = phase0Published::countDown;

    final List<Throwable> appendFailure = new ArrayList<>();
    final List<Throwable> compactionFailure = new ArrayList<>();
    final Thread appender = new Thread(() -> {
      try {
        append(followerDb, 3_000L);
      } catch (final Throwable t) {
        appendFailure.add(t);
      }
    }, "issue11921-follower-append");
    final Thread compactor = new Thread(() -> {
      try {
        ((LocalTimeSeriesType) leaderDb.getSchema().getType(TYPE)).getEngine().compactAll();
      } catch (final Throwable t) {
        compactionFailure.add(t);
      }
    }, "issue11921-compaction");

    // Daemons: a worker stuck past its join fails the test below and must not keep the fork alive
    appender.setDaemon(true);
    compactor.setDaemon(true);

    boolean phase0Reached = false;
    boolean compactionQueued = false;
    validationLock.lock();
    try {
      appender.start();
      assertThat(awaitValidationBlocked()).as("the leader's validation of the follower append must reach the lock").isTrue();
      compactor.start();
      // The compaction either publishes phase 0 (the hook runs after it, before phase 4c) without the validation lock, which
      // is the race, or reaches its first version check and queues on the lock the test holds. Waiting for one of the two,
      // not for a fixed time, keeps the outcome from depending on how long the compaction takes to get there.
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (System.nanoTime() < deadline && compactor.isAlive()) {
        phase0Reached = phase0Published.getCount() == 0;
        compactionQueued = validationLock.hasQueuedThread(compactor);
        if (phase0Reached || compactionQueued)
          break;
        Thread.sleep(5);
      }
    } finally {
      validationLock.unlock();
    }
    appender.join(60_000);
    compactor.join(60_000);
    // The log is read as the outcome of both: one still running could add an entry after the scan
    assertThat(appender.isAlive()).as("the follower append must have ended before the log is read").isFalse();
    assertThat(compactor.isAlive()).as("the compaction must have ended before the log is read").isFalse();
    waitForAllServers();

    final Map<String, Long> claims = new HashMap<>();
    final List<String> duplicates = new ArrayList<>();
    final RaftLog log = leaderRaft.getRaftDivision().getRaftLog();
    for (long i = logBefore + 1; i <= log.getLastEntryTermIndex().getIndex(); i++) {
      final LogEntryProto entry = log.get(i);
      if (entry == null || !entry.hasStateMachineLogEntry() || entry.getStateMachineLogEntry().getLogData().isEmpty())
        continue;
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry.getStateMachineLogEntry().getLogData());
      if (!getDatabaseName().equals(decoded.databaseName()))
        continue;
      final List<byte[]> wals = new ArrayList<>();
      if (decoded.type() == RaftLogEntryType.TX_ENTRY)
        wals.add(decoded.walData());
      else if (decoded.type() == RaftLogEntryType.SCHEMA_ENTRY && decoded.walEntries() != null)
        wals.addAll(decoded.walEntries());
      for (final byte[] wal : wals) {
        final PageVersionLedger.Pages pages = PageVersionLedger.parse(wal);
        final Map<String, Long> ofThisWal = new HashMap<>();
        for (int p = 0; p < pages.count(); p++)
          ofThisWal.put(pages.fileIds()[p] + "/" + pages.pageNumbers()[p] + " v" + pages.versions()[p], i);
        for (final Map.Entry<String, Long> claim : ofThisWal.entrySet()) {
          final Long previous = claims.putIfAbsent(claim.getKey(), claim.getValue());
          if (previous != null)
            duplicates.add("page " + claim.getKey() + " claimed by entries " + previous + " and " + claim.getValue());
        }
      }
    }

    LogManager.instance().log(this, Level.INFO, "TEST: phase 0 published while the validation waited=%s, compaction queued on "
            + "the validation lock=%s, append failure=%s, compaction failure=%s, claims=%s", phase0Reached, compactionQueued,
        appendFailure, compactionFailure, claims);
    assertThat(phase0Reached).as("the compaction published phase 0 while the leader's validation lock was held").isFalse();
    assertThat(compactionQueued).as("the compaction must reach its first version check and wait for the validation lock; "
        + "compaction failure: %s", compactionFailure).isTrue();
    assertThat(duplicates).as("append failure: %s, compaction failure: %s", appendFailure, compactionFailure).isEmpty();
    assertClusterConsistency();
  }

  private void append(final Database db, final long ts) throws Exception {
    ((LocalTimeSeriesType) db.getSchema().getType(TYPE)).getEngine().appendSamples(new long[] { ts }, new Object[][] { { ts } });
  }

  /** The leader ledger's per-database lock that {@code validateAndReserve} runs under. */
  private ReentrantLock validationLock(final ArcadeStateMachine stateMachine) {
    final ReentrantLock lock = stateMachine.pageVersions.validationLockForTesting(getDatabaseName());
    assertThat(lock).as("the follower append above created the leader's ledger of the database").isNotNull();
    return lock;
  }

  private static boolean awaitValidationBlocked() throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      for (final Map.Entry<Thread, StackTraceElement[]> thread : Thread.getAllStackTraces().entrySet())
        if (thread.getKey().getState() == Thread.State.WAITING || thread.getKey().getState() == Thread.State.BLOCKED)
          for (final StackTraceElement frame : thread.getValue())
            if (frame.getClassName().equals(PageVersionLedger.class.getName()) && frame.getMethodName().equals("validateAndReserve"))
              return true;
      Thread.sleep(5);
    }
    return false;
  }
}
