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
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.*;

/**
 * Verifies that writes issued via HTTP against a Raft follower are forwarded to the leader and
 * replicated to all nodes. In the Raft implementation, DML commands sent via the HTTP command
 * endpoint on a follower are forwarded to the leader, committed there, and then propagated to all
 * replicas via the Raft log.
 */
class RaftReplicationWriteAgainstReplicaIT extends BaseRaftHATest {

  private static final int    TXS          = 50; // sufficient to verify HTTP write forwarding
  private static final String REPLICA_TYPE = "RaftReplicaWrite";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void writesForwardedFromReplicaToLeader() throws Exception {
    // Find a follower (non-leader) server index
    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && !plugin.isLeader()) {
        followerIndex = i;
        break;
      }
    }
    assertThat(followerIndex).as("Expected to find a follower node").isGreaterThanOrEqualTo(0);
    LogManager.instance().log(this, Level.INFO, "Writing against follower node %d", followerIndex);

    // Create a vertex type via HTTP on the follower (DDL is forwarded to the leader)
    command(followerIndex, "CREATE VERTEX TYPE " + REPLICA_TYPE);

    // Wait for schema replication to all nodes
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(REPLICA_TYPE))
          .as("Server %d should have type %s after schema replication", i, REPLICA_TYPE).isTrue();

    // Insert records via HTTP on the follower; the HTTP handler forwards DML to the leader
    for (int i = 0; i < TXS; i++)
      command(followerIndex, "INSERT INTO " + REPLICA_TYPE + " SET seq = " + i + ", name = 'replica-write-test'");

    final long expectedCount = TXS;
    LogManager.instance().log(this, Level.INFO, "Issued %d inserts via follower HTTP, waiting for replication", expectedCount);

    // Wait for all nodes to catch up
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify all nodes see the same count
    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      final long[] count = { 0 };
      serverDb.transaction(() -> count[0] = serverDb.countType(REPLICA_TYPE, true));
      assertThat(count[0]).as("Server %d should have %d vertices of type %s", i, expectedCount, REPLICA_TYPE)
          .isEqualTo(expectedCount);
    }
  }
}
