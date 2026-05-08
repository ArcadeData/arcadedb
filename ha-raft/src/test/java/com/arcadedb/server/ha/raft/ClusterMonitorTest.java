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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterMonitorTest {

  private CapturingLogger captured;
  private Logger          previousLogger;

  @BeforeEach
  void installCapturingLogger() {
    captured = new CapturingLogger();
    // Save the current ArcadeDB logger so we can restore it after the test (other tests in the
    // same JVM may rely on the standard DefaultLogger).
    previousLogger = new DefaultLogger();
    LogManager.instance().setLogger(captured);
  }

  @AfterEach
  void restoreLogger() {
    LogManager.instance().setLogger(previousLogger);
  }

  @Test
  void replicationLagIsZeroWhenEmpty() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void tracksReplicaLag() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 90);
    monitor.updateReplicaMatchIndex("replica2", 50);

    final Map<String, Long> lags = monitor.getReplicaLags();
    assertThat(lags).containsEntry("replica1", 10L);
    assertThat(lags).containsEntry("replica2", 50L);
  }

  @Test
  void identifiesLaggingReplicas() {
    final ClusterMonitor monitor = new ClusterMonitor(20L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 90);
    monitor.updateReplicaMatchIndex("replica2", 50);

    assertThat(monitor.isReplicaLagging("replica1")).isFalse();
    assertThat(monitor.isReplicaLagging("replica2")).isTrue();
  }

  @Test
  void removesReplica() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateReplicaMatchIndex("replica1", 50);
    monitor.removeReplica("replica1");
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void lagUpdatesWhenLeaderAdvances() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 95);
    assertThat(monitor.getReplicaLags().get("replica1")).isEqualTo(5L);

    monitor.updateLeaderCommitIndex(200);
    assertThat(monitor.getReplicaLags().get("replica1")).isEqualTo(105L);
  }

  /**
   * Replica's matchIndex doesn't advance while the leader's commit index grows = pre-churn signal.
   * Logged at SEVERE so it stands out in the operator's log.
   */
  @Test
  void stalledReplicaLogsSevereOnce() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    // Tick 1: seed state - replica is at 100, leader at 100, no lag.
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100);
    captured.clear(); // ignore the seeding tick

    // Tick 2: leader advances 1000 entries, replica is stuck at 100. Lag now 1000 (well over 50).
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100);

    final List<CapturedLine> severe = captured.linesAtLevel(Level.SEVERE);
    assertThat(severe).hasSize(1);
    assertThat(severe.get(0).message).contains("STALLED").contains("replica1");
  }

  /**
   * Lag is growing tick-over-tick: WARNING (not SEVERE). Throttled to one log per 30s.
   */
  @Test
  void growingLagLogsWarning() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    // Seed: replica at 100, leader at 100.
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100);
    captured.clear();

    // Tick: leader +500, replica +200. Replica IS advancing but slower. Lag = 300 (over 50).
    monitor.updateLeaderCommitIndex(600);
    monitor.updateReplicaMatchIndex("replica1", 300);

    final List<CapturedLine> warns = captured.linesAtLevel(Level.WARNING);
    assertThat(warns).hasSize(1);
    assertThat(warns.get(0).message).contains("falling behind").contains("replica1");
    assertThat(captured.linesAtLevel(Level.SEVERE)).isEmpty();
  }

  /**
   * Healthy lag (<= threshold): no log. Confirms we don't spam when there's nothing to say.
   */
  @Test
  void healthyLagDoesNotLog() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 95);
    captured.clear();

    monitor.updateLeaderCommitIndex(200);
    monitor.updateReplicaMatchIndex("replica1", 195);

    assertThat(captured.lines).isEmpty();
  }

  /**
   * Same replica reported twice in quick succession produces only one log line.
   */
  @Test
  void perReplicaThrottlePreventsSpam() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100);
    captured.clear();

    // First over-threshold tick: should log.
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100);
    final int afterFirst = captured.lines.size();
    assertThat(afterFirst).isEqualTo(1);

    // Second over-threshold tick within the 30s throttle window: should NOT log.
    monitor.updateLeaderCommitIndex(2100);
    monitor.updateReplicaMatchIndex("replica1", 100);
    assertThat(captured.lines).hasSize(afterFirst);
  }

  /** ArcadeDB Logger that captures everything in memory for test assertions. */
  private static final class CapturingLogger implements Logger {
    final List<CapturedLine> lines = new ArrayList<>();

    @Override
    public void log(final Object req, final Level level, final String msg, final Throwable t, final String ctx,
        final Object a1, final Object a2, final Object a3, final Object a4, final Object a5,
        final Object a6, final Object a7, final Object a8, final Object a9, final Object a10,
        final Object a11, final Object a12, final Object a13, final Object a14, final Object a15,
        final Object a16, final Object a17) {
      final boolean hasParams = a1 != null || a2 != null || a3 != null || a4 != null || a5 != null
          || a6 != null || a7 != null || a8 != null || a9 != null || a10 != null
          || a11 != null || a12 != null || a13 != null || a14 != null || a15 != null
          || a16 != null || a17 != null;
      final String formatted = hasParams
          ? msg.formatted(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17)
          : msg;
      lines.add(new CapturedLine(level, formatted));
    }

    @Override
    public void log(final Object req, final Level level, final String msg, final Throwable t, final String ctx,
        final Object... args) {
      lines.add(new CapturedLine(level, args.length > 0 ? msg.formatted(args) : msg));
    }

    @Override
    public void flush() {
    }

    void clear() {
      lines.clear();
    }

    List<CapturedLine> linesAtLevel(final Level level) {
      return lines.stream().filter(l -> l.level.equals(level)).toList();
    }
  }

  private record CapturedLine(Level level, String message) {
  }
}
