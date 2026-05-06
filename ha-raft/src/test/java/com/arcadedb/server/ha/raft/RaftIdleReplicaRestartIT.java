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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Minimal reproduction for the variant of issue #4081 reported by mdre on 2026-05-06: an idle
 * 4-node cluster (no data inserts), default {@code raftPersistStorage=false}, stop one replica
 * and let the leader log "claiming for nodo4" warnings for a few seconds, then restart the
 * replica. Expected: the cluster converges and the leader stops emitting
 * {@code received INCONSISTENCY reply with nextIndex 0, errorCount=N, ..., entriesCount=0}
 * warnings every 5-6s.
 * <p>
 * The earlier {@code RaftReplicaCrashAndRecoverIT} did not catch this because it uses
 * {@code persistentRaftStorage=true} (so the replica's Raft log survives the restart and Ratis
 * can resume from a known matchIndex), and because the test inserts data first (so the leader
 * has actual log entries to ship). mdre's setup hits the empty-log + ephemeral-storage path
 * which is what exposes the bug.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftIdleReplicaRestartIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 4;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // mdre's setup uses the default (false). Storage is wiped on every restart - the replica
    // comes back with an empty Raft log and must catch up from the leader.
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected void populateDatabase() {
    // Skip the default test population. The bug shows up against an idle cluster with no data
    // inserts - the leader's only log entries are configuration ones.
  }

  @Test
  void idleReplicaRestartConverges() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Pick a non-leader replica.
    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    }
    assertThat(replicaIndex).as("a replica must exist").isGreaterThanOrEqualTo(0);

    LogManager.instance().log(this, Level.INFO, "TEST: stopping replica server %d", replicaIndex);
    getServer(replicaIndex).stop();
    while (getServer(replicaIndex).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(200);

    // Stay offline long enough for the leader to settle into its retry/backoff cadence and emit
    // the "claiming for replica" warnings mdre describes ("every few seconds"). This timing
    // mirrors his manual reproduction better than a 5 s window.
    CodeUtils.sleep(30_000);

    LogManager.instance().log(this, Level.INFO, "TEST: restarting replica server %d", replicaIndex);
    getServer(replicaIndex).start();

    // Attach a log handler that counts the exact warning mdre is seeing every 5-6 s. mdre's
    // signature is "received INCONSISTENCY reply with nextIndex 0, errorCount=N,
    // request=AppendEntriesRequest:cid=M,entriesCount=0" emitted from the leader's GrpcLogAppender.
    // A handful of these are normal during the brief reconnect window; many of them, sustained,
    // mean the leader is stuck and the cluster will never converge.
    final InconsistencyLoopWatcher watcher = InconsistencyLoopWatcher.attach();

    // Wait for the cluster to converge: every server's last-applied index must reach the leader's.
    waitForReplicationIsCompleted(replicaIndex);

    // Mark AFTER catch-up. The transient warnings emitted while gRPC reconnects are not the bug;
    // sustained warnings AFTER catch-up are. Reset the counter to zero before the observation window.
    watcher.mark();

    // Observe for a further 15 s once the replica has caught up. If the leader is logging the
    // INCONSISTENCY-with-nextIndex-0/entriesCount-0 warning every 5-6 s during this window, the
    // cluster is in mdre's stuck state.
    CodeUtils.sleep(15_000);

    final int loopCountAfterCatchup = watcher.countSinceMark();
    watcher.detach();
    LogManager.instance().log(this, Level.INFO,
        "TEST: observed %d INCONSISTENCY-with-nextIndex-0 warnings in the 15s post-catchup window",
        loopCountAfterCatchup);

    assertThat(loopCountAfterCatchup)
        .as("after the replica caught up, the leader must stop emitting the "
            + "'INCONSISTENCY reply with nextIndex 0 ... entriesCount=0' warning. "
            + "Observed %d such warnings in the 15s observation window - leader is stuck", loopCountAfterCatchup)
        .isLessThanOrEqualTo(2);

    assertClusterConsistency();

    // The restarted replica must have the post-restart data.
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("PostRestart", true))
        .as("restarted replica must replicate writes that happened after it came back")
        .isEqualTo(10L);
  }

  /**
   * java.util.logging handler that counts {@code GrpcLogAppender} warnings whose message contains
   * the {@code received INCONSISTENCY reply with nextIndex 0} ... {@code entriesCount=0} signature.
   * Provides a {@link #countSinceMark()} method so the test can ignore transient warnings during
   * reconnect and only fail when the warning persists past the catch-up window.
   */
  private static final class InconsistencyLoopWatcher extends Handler {
    private final List<String> matches = new ArrayList<>();
    private final AtomicInteger markedCount = new AtomicInteger();
    private final Logger        root        = Logger.getLogger("");
    private final Level         previousLevel = root.getLevel();

    static InconsistencyLoopWatcher attach() {
      final InconsistencyLoopWatcher h = new InconsistencyLoopWatcher();
      h.setLevel(Level.WARNING);
      h.root.addHandler(h);
      if (h.root.getLevel() == null || h.root.getLevel().intValue() > Level.WARNING.intValue())
        h.root.setLevel(Level.WARNING);
      return h;
    }

    /** Snapshots the current count so {@link #countSinceMark()} returns 0 immediately after. */
    void mark() {
      synchronized (matches) {
        markedCount.set(matches.size());
      }
    }

    int countSinceMark() {
      synchronized (matches) {
        return matches.size() - markedCount.get();
      }
    }

    void detach() {
      root.removeHandler(this);
      if (previousLevel != null)
        root.setLevel(previousLevel);
    }

    @Override
    public void publish(final LogRecord record) {
      if (record == null || record.getMessage() == null)
        return;
      final String msg = record.getMessage();
      if (msg.contains("received INCONSISTENCY reply") && msg.contains("nextIndex 0") && msg.contains("entriesCount=0")) {
        synchronized (matches) {
          matches.add(msg);
        }
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }
}
