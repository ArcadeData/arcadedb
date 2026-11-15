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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    waitForAllServers();
    for (int i = 0; i < 20; i++)
      leaderDb.command("sql", "INSERT INTO " + TYPE + " SET ts = ?, v = ?", 1_000L + i, (long) i);
    // One append from the follower, so the leader holds a ledger for the database
    append(followerDb, 2_000L);
    waitForAllServers();

    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    final Object monitor = validationMonitor(leaderRaft.getStateMachine());
    final long logBefore = lastRaftLogIndex(leaderRaft);

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

    synchronized (monitor) {
      appender.start();
      assertThat(awaitValidationBlocked()).as("the leader's validation of the follower append must reach the monitor").isTrue();
      compactor.start();
      // Phase 0 published (the hook runs after it, before phase 4c). A compaction that has to take the monitor itself
      // cannot get here while the test holds it: the wait then times out and the two race for the monitor below.
      phase0Published.await(3, TimeUnit.SECONDS);
    }
    appender.join(60_000);
    compactor.join(60_000);
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

    assertThat(duplicates).as("append failure: %s, compaction failure: %s", appendFailure, compactionFailure).isEmpty();
    assertClusterConsistency();
  }

  private void append(final Database db, final long ts) throws Exception {
    ((LocalTimeSeriesType) db.getSchema().getType(TYPE)).getEngine().appendSamples(new long[] { ts }, new Object[][] { { ts } });
  }

  /** The leader ledger's per-database monitor that {@code validateAndReserve} runs under. */
  private Object validationMonitor(final ArcadeStateMachine stateMachine) throws Exception {
    final Field ledgerField = ArcadeStateMachine.class.getDeclaredField("pageVersions");
    ledgerField.setAccessible(true);
    final Field databases = PageVersionLedger.class.getDeclaredField("byDatabase");
    databases.setAccessible(true);
    final Object monitor = ((Map<?, ?>) databases.get(ledgerField.get(stateMachine))).get(getDatabaseName());
    assertThat(monitor).as("the follower append above created the leader's ledger of the database").isNotNull();
    return monitor;
  }

  private static boolean awaitValidationBlocked() throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      for (final Map.Entry<Thread, StackTraceElement[]> thread : Thread.getAllStackTraces().entrySet())
        if (thread.getKey().getState() == Thread.State.BLOCKED)
          for (final StackTraceElement frame : thread.getValue())
            if (frame.getClassName().equals(PageVersionLedger.class.getName()) && frame.getMethodName().equals("validateAndReserve"))
              return true;
      Thread.sleep(5);
    }
    return false;
  }
}
