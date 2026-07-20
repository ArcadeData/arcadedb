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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of the production wiring behind the periodic Raft log compaction (issue #5345),
 * against a real {@link RaftHAServer} cluster.
 * <p>
 * {@link RaftPeriodicSnapshotCompactionIT} proves the Ratis mechanism itself but builds the request
 * itself against a mini-cluster; the unit tests drive {@link RaftLogCompactionScheduler} against a
 * fake. This test covers the seam between them - {@code RaftHAServer.takeLocalSnapshot()} assembling
 * the request from the real client id, local peer id and group id, and the compaction target resolving
 * the Raft storage volume for the free-space probe. A wrong group id or an unresolvable storage volume
 * would be invisible to both of the other suites.
 */
@Tag("slow")
class RaftLogCompactionWiringIT extends BaseRaftHATest {

  @Test
  void takeLocalSnapshotSucceedsOnEveryNodeThroughTheProductionRequestPath() throws Exception {
    // Give the state machine something to snapshot, and let every peer apply it.
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      assertThat(plugin).as("server %d must expose a Raft plugin", i).isNotNull();

      final RaftHAServer haServer = plugin.getRaftHAServer();
      assertThat(haServer).as("server %d must expose a Raft HA server", i).isNotNull();

      // A creation gap of 1 is what the scheduler uses under disk pressure. Leader and followers alike
      // must accept the request: each node purges its own log against its own snapshot index.
      final long snapshotIndex = haServer.takeLocalSnapshot(1L);
      assertThat(snapshotIndex)
          .as("server %d (leader=%s) must take a local snapshot through the production request path",
              i, haServer.isLeader())
          .isNotNegative();
    }
  }

  @Test
  void compactionTargetResolvesTheRaftStorageVolumeForTheFreeSpaceProbe() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAServer haServer = getRaftPlugin(i).getRaftHAServer();
      final RaftLogCompactionScheduler.CompactionTarget target = haServer.newCompactionTargetForTesting();

      // A zero or negative total would make the scheduler read the volume as "size unknown" and disable
      // the disk-pressure escalation entirely, silently losing the guard against the log filling the disk.
      assertThat(target.getRaftStorageTotalSpaceBytes())
          .as("server %d must resolve a real Raft storage volume size", i)
          .isPositive();
      assertThat(target.getRaftStorageUsableSpaceBytes())
          .as("server %d must report free space on the Raft storage volume", i)
          .isNotNegative();
      assertThat(target.getRaftStorageDescription())
          .as("server %d must name the Raft storage location for the disk-pressure warning", i)
          .isNotBlank()
          .isNotEqualTo("<unknown>");
      assertThat(target.isShutdownRequested())
          .as("server %d is running, so the tick must not be suppressed", i)
          .isFalse();
    }
  }
}
