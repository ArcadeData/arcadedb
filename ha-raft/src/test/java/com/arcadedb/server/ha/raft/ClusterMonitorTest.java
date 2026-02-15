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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterMonitorTest {

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
}
