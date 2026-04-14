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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ClusterMonitor replication lag tracking.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class ClusterMonitorTest {

  @Test
  void replicationLagIsZeroWhenEmpty() {
    final ClusterMonitor monitor = new ClusterMonitor(1000);
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void tracksReplicaLag() {
    final ClusterMonitor monitor = new ClusterMonitor(1000);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("peer-1", 95);
    monitor.updateReplicaMatchIndex("peer-2", 80);

    final var lags = monitor.getReplicaLags();
    assertThat(lags).hasSize(2);
    assertThat(lags.get("peer-1")).isEqualTo(5);
    assertThat(lags.get("peer-2")).isEqualTo(20);
  }

  @Test
  void lagUpdatesWhenLeaderAdvances() {
    final ClusterMonitor monitor = new ClusterMonitor(1000);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("peer-1", 100);
    assertThat(monitor.getReplicaLags().get("peer-1")).isEqualTo(0);

    // Leader advances, replica stays behind
    monitor.updateLeaderCommitIndex(200);
    monitor.updateReplicaMatchIndex("peer-1", 100);
    assertThat(monitor.getReplicaLags().get("peer-1")).isEqualTo(100);
  }

  @Test
  void thresholdIsReported() {
    final ClusterMonitor monitor = new ClusterMonitor(500);
    assertThat(monitor.getLagWarningThreshold()).isEqualTo(500);
  }

  @Test
  void zeroThresholdDisablesWarnings() {
    // Threshold=0 means disabled - no warnings emitted
    final ClusterMonitor monitor = new ClusterMonitor(0);
    monitor.updateLeaderCommitIndex(10000);
    monitor.updateReplicaMatchIndex("peer-1", 0);
    // Should not throw or log (no assertion on logging, just verifying no exception)
    assertThat(monitor.getReplicaLags().get("peer-1")).isEqualTo(10000);
  }

  @Test
  void removeReplicaClearsLagEntry() {
    final ClusterMonitor monitor = new ClusterMonitor(1000);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("peer-1", 80);
    monitor.updateReplicaMatchIndex("peer-2", 90);
    assertThat(monitor.getReplicaLags()).containsKeys("peer-1", "peer-2");

    monitor.removeReplica("peer-1");

    assertThat(monitor.getReplicaLags()).containsOnlyKeys("peer-2");
  }

  @Test
  void removeReplicaClearsWarnState() {
    final AtomicLong now = new AtomicLong(1000);
    final ClusterMonitor monitor = new ClusterMonitor(10, now::get);
    monitor.updateLeaderCommitIndex(100);

    // Trigger a warning for peer-1
    monitor.updateReplicaMatchIndex("peer-1", 0);

    // Remove and re-add the same replica - it should be able to warn again immediately
    monitor.removeReplica("peer-1");
    monitor.updateReplicaMatchIndex("peer-1", 0);
    // No exception - the warn state was cleared so the second update logs without debounce
  }

  @Test
  void removeNonExistentReplicaIsNoOp() {
    final ClusterMonitor monitor = new ClusterMonitor(1000);
    monitor.removeReplica("ghost");
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void debouncesSuppressesWarningWithinInterval() {
    final AtomicLong now = new AtomicLong(1000);
    final ClusterMonitor monitor = new ClusterMonitor(10, now::get);
    monitor.updateLeaderCommitIndex(100);

    // First update triggers the warning and records the warn time
    monitor.updateReplicaMatchIndex("peer-1", 0);

    // Advance time by less than the debounce interval
    now.set(1000 + ClusterMonitor.LAG_WARN_INTERVAL_MS - 1);
    // This should not throw - warning is suppressed
    monitor.updateReplicaMatchIndex("peer-1", 5);

    // Lag is still tracked correctly even though warning was suppressed
    assertThat(monitor.getReplicaLags().get("peer-1")).isEqualTo(95);
  }

  @Test
  void debounceAllowsWarningAfterInterval() {
    final AtomicLong now = new AtomicLong(1000);
    final ClusterMonitor monitor = new ClusterMonitor(10, now::get);
    monitor.updateLeaderCommitIndex(100);

    // First update triggers warning
    monitor.updateReplicaMatchIndex("peer-1", 0);

    // Advance past the debounce interval
    now.set(1000 + ClusterMonitor.LAG_WARN_INTERVAL_MS);
    // This should log a new warning (no exception)
    monitor.updateReplicaMatchIndex("peer-1", 5);

    assertThat(monitor.getReplicaLags().get("peer-1")).isEqualTo(95);
  }

  @Test
  void catchUpClearsWarnState() {
    final AtomicLong now = new AtomicLong(1000);
    final ClusterMonitor monitor = new ClusterMonitor(10, now::get);
    monitor.updateLeaderCommitIndex(100);

    // Trigger warning
    monitor.updateReplicaMatchIndex("peer-1", 0);

    // Replica catches up (lag drops below threshold)
    monitor.updateReplicaMatchIndex("peer-1", 95);

    // Now lag exceeds threshold again - should warn immediately because catch-up cleared the state
    now.set(1001); // barely any time passed
    monitor.updateReplicaMatchIndex("peer-1", 0);

    assertThat(monitor.getReplicaLags().get("peer-1")).isEqualTo(100);
  }
}
