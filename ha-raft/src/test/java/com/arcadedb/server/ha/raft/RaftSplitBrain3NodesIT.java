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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster with majority quorum.
 * Tests split-brain scenario: network partition isolates the leader from the majority.
 * <p>
 * Expected behavior in a real split-brain scenario:
 * <ul>
 *   <li>The isolated leader loses its leadership because it cannot reach a quorum.</li>
 *   <li>The majority partition elects a new leader.</li>
 *   <li>Writes on the isolated (old) leader fail because it can no longer commit to a quorum.</li>
 *   <li>Writes on the new leader succeed because the majority partition has quorum.</li>
 *   <li>When the partition heals, the old leader discovers the new term and steps down.</li>
 *   <li>Any uncommitted entries on the old leader are discarded; Raft guarantees consistency.</li>
 * </ul>
 * <p>
 * This test is disabled because true network partition simulation requires Ratis test utilities
 * (e.g., MiniRaftCluster with simulated network partitions) that are not available in the
 * current test infrastructure. The in-JVM test setup shares the same network stack, so we
 * cannot selectively block messages between specific peers.
 */
@Disabled("Requires Ratis MiniRaftCluster or network simulation utilities for true partition testing")
class RaftSplitBrain3NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void splitBrainMajorityPartitionElectsNewLeader() {
    // This test documents the expected behavior for a split-brain scenario.
    // When network partition simulation utilities become available, this test should:
    //
    // 1. Write initial data on the leader
    // 2. Partition the network: isolate the leader from the other 2 nodes
    // 3. Verify: the isolated leader eventually loses leadership (cannot maintain quorum)
    // 4. Verify: the majority partition (2 nodes) elects a new leader
    // 5. Write data on the new leader (should succeed)
    // 6. Attempt write on the isolated old leader (should fail/timeout)
    // 7. Heal the partition
    // 8. Verify: the old leader discovers the new term and steps down
    // 9. Verify: all nodes converge to the same state (data from the new leader wins)

    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
  }

  private int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }
}
