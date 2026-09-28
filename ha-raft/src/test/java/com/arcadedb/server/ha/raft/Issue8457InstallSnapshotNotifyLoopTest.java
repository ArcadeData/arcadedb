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

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8457 (residual of #8449): a follower whose {@code nextIndex} has fallen at or below the leader's own
 * compacted log start cannot be caught up by ordinary {@code AppendEntries} - {@code LogAppender.shouldInstallSnapshot}
 * keeps re-notifying the same install-snapshot boundary, and the follower keeps answering {@code ALREADY_INSTALLED}
 * without ever calling its state machine again. The numeric replication lag in that state can be small enough to
 * hide under {@code arcadedb.ha.replicationLagWarning} forever, so neither the plain lag classification nor the
 * leader-driven stalled-replica resync (both gated on a large lag) ever engaged.
 */
class Issue8457InstallSnapshotNotifyLoopTest {

  private static final String REPLICA = "replica1";

  /**
   * The exact shape from the issue: matchIndex/nextIndex sit just behind a leader that has purged its log to
   * start at nextIndex itself (the #8449 boundary+1 case), while the numeric lag against the leader's commit
   * index stays comfortably under the warning threshold. Must still be reported STALLED once the condition
   * outlasts the grace, not masked as HEALTHY.
   */
  @Test
  void nextIndexAtLeaderLogStartIsStalledEvenWithSmallLag() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    // leaderLogStartIndex=100, this replica's nextIndex=100 (== log start, the boundary+1 case), matchIndex=99.
    // lag = 105 - 99 = 6, far under the 1000 warning threshold.
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("still inside the grace")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    now.set(59_999);
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("1ms short of the grace")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    now.set(60_000);
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /**
   * The leader-driven resync (POST /api/v1/cluster/resync/{database} under the hood) must NOT fire for this
   * condition: it only replaces database files over HTTP and never touches the Raft-log position that is
   * actually stuck here, so firing it would burn a full database re-download for nothing while leaving the
   * notify loop exactly as stuck as before. This documents that deliberate limitation - see
   * {@code ClusterMonitor#trackStallForRecovery} - rather than a bug: recovering this condition today is a
   * manual, operator-driven step (remove/re-add the peer, or wipe its Raft storage and restart it).
   */
  @Test
  void neverTriggersTheFileOnlyResyncWhichCannotClearTheCondition() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, resynced::add);
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(resynced).isEmpty();

    now.set(60_000);
    monitor.updateLeaderCommitIndex(106);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("still reported STALLED").isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
    assertThat(resynced).as("no auto-recovery attempted for this specific condition").isEmpty();

    now.set(600_000);
    monitor.updateLeaderCommitIndex(107);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(resynced).as("stays empty however long the condition persists").isEmpty();
  }

  /** A follower whose nextIndex is strictly below the leader's log start is the same condition, not a milder one. */
  @Test
  void nextIndexBelowLeaderLogStartIsAlsoStalled() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(500);
    monitor.updateReplicaMatchIndex(REPLICA, 50, 0L, 51, 200);
    now.set(60_000);
    monitor.updateLeaderCommitIndex(500);
    monitor.updateReplicaMatchIndex(REPLICA, 50, 0L, 51, 200);
    assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /**
   * A partitioned follower can sit at the same nextIndex-behind-log-start position, but it answers nothing, while a
   * follower in the notify loop keeps replying ALREADY_INSTALLED. The partition must not be diagnosed as the loop,
   * whose remediation (wipe the Raft storage) is destructive and useless for a network problem.
   */
  @Test
  void unreachableFollowerIsNotDiagnosedAsTheNotifyLoop() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    }, false, 10_000L);
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(5_000);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 60_000L, 100, 100);
    now.set(120_000);
    monitor.updateLeaderCommitIndex(5_000);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 120_000L, 100, 100);
    // lag 4901 > threshold, so the lag-warning switch is reached and picks a STALLED message.
    // STALLED through the ordinary lag rules is fine; what must not happen is the notify-loop diagnosis.
    final List<String> messages = new ArrayList<>();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object req, final Level level, final String msg, final Throwable t, final String ctx,
          final Object a1, final Object a2, final Object a3, final Object a4, final Object a5, final Object a6,
          final Object a7, final Object a8, final Object a9, final Object a10, final Object a11, final Object a12,
          final Object a13, final Object a14, final Object a15, final Object a16, final Object a17) {
        messages.add(msg);
      }

      @Override
      public void log(final Object req, final Level level, final String msg, final Throwable t, final String ctx,
          final Object... args) {
        messages.add(msg);
      }

      @Override
      public void flush() {
      }
    });
    try {
      now.set(240_000);
      monitor.updateLeaderCommitIndex(5_000);
      monitor.updateReplicaMatchIndex(REPLICA, 99, 240_000L, 100, 100);
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
    }
    assertThat(messages).noneMatch(m -> m.contains("install-snapshot notify loop"));
  }

  /** A brand-new, never-compacted leader log (start index 0) must never be misread as this condition. */
  @Test
  void freshEmptyLogNeverMisclassifiesAsInstallSnapshotLoop() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    // matchIndex=0 (not the never-appended sentinel -1, issue #5295's own condition), leaderLogStartIndex=0:
    // an empty, never-compacted log, which nextIndex <= 0 would otherwise misread as this condition.
    monitor.updateLeaderCommitIndex(0);
    monitor.updateReplicaMatchIndex(REPLICA, 0, 0L, 0, 0);
    now.set(120_000);
    monitor.updateLeaderCommitIndex(0);
    monitor.updateReplicaMatchIndex(REPLICA, 0, 0L, 0, 0);
    assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
  }

  /** Unknown nextIndex or leader log start (-1, e.g. a degraded follower-state read) must skip the check entirely. */
  @Test
  void unknownNextIndexOrLogStartSkipsTheCheck() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, -1, 100);
    now.set(120_000);
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, -1, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("unknown nextIndex")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    monitor.reset();
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, -1);
    now.set(240_000);
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, -1);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("unknown leader log start")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /** The 3-arg overload (existing callers, e.g. tests that never learned about #8457) must behave exactly as before. */
  @Test
  void threeArgOverloadNeverTriggersTheNewCondition() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L);
    now.set(120_000);
    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L);
    assertThat(monitor.getReplicaStatus(REPLICA)).isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /**
   * A legitimate, in-progress snapshot install also has nextIndex at or below the leader's log start for a
   * while; if it completes (nextIndex clears the boundary) before the grace elapses, the replica must never be
   * reported STALLED for this condition - the grace exists precisely to absorb a normal install in flight.
   */
  @Test
  void installThatCompletesBeforeTheGraceNeverMisreportsStalled() {
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(1000L, 60_000L, id -> {
    });
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(105);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);

    now.set(30_000);
    monitor.updateLeaderCommitIndex(106);
    monitor.updateReplicaMatchIndex(REPLICA, 99, 0L, 100, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("still inside the grace")
        .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

    // Install completes: nextIndex jumps past the leader's log start.
    now.set(35_000);
    monitor.updateLeaderCommitIndex(106);
    monitor.updateReplicaMatchIndex(REPLICA, 105, 0L, 106, 100);

    now.set(120_000);
    monitor.updateLeaderCommitIndex(106);
    monitor.updateReplicaMatchIndex(REPLICA, 105, 0L, 106, 100);
    assertThat(monitor.getReplicaStatus(REPLICA)).as("caught up long ago")
        .isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
  }
}
