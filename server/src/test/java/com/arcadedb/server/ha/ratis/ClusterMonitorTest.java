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
package com.arcadedb.server.ha.ratis;

import org.junit.jupiter.api.Test;

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
}
