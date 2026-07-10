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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies that writes issued through the <b>embedded</b> API (i.e. directly on the
 * {@link com.arcadedb.server.ServerDatabase} returned by the server, without going through the HTTP
 * layer) against a Raft follower are forwarded to the leader and replicated to all nodes.
 * <p>
 * Embedded callers (e.g. an embedded scripting engine or an application background/worker thread)
 * run with no per-thread security context because they never pass through the HTTP handler that
 * binds the request user to the thread-local {@code DatabaseContext}. Before the fix this caused a
 * {@code SecurityException: Cannot forward command to leader: no authenticated user in the current
 * security context} when the follower tried to forward the write to the leader. The follower now
 * forwards such trusted in-process writes as the {@code root} user.
 */
class RaftEmbeddedWriteAgainstReplicaIT extends BaseRaftHATest {

  private static final int    TXS          = 20; // enough to verify embedded write forwarding
  private static final String REPLICA_TYPE = "RaftEmbeddedReplicaWrite";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void embeddedWritesForwardedFromReplicaToLeader() throws Exception {
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
    LogManager.instance().log(this, Level.INFO, "Writing against follower node %d via the embedded API", followerIndex);

    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());

    // DDL issued through the embedded API on a follower must be forwarded to the leader even though
    // there is no security context on this (test) thread.
    final int fIdx = followerIndex;
    assertThatCode(() -> followerDb.command("sql", "CREATE VERTEX TYPE " + REPLICA_TYPE))
        .as("Embedded DDL on follower %d should be forwarded to the leader, not fail with SecurityException", fIdx)
        .doesNotThrowAnyException();

    // Wait for schema replication to all nodes
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(REPLICA_TYPE))
          .as("Server %d should have type %s after schema replication", i, REPLICA_TYPE).isTrue();

    // Insert records through the embedded API on the follower; the non-idempotent DML must be
    // forwarded to the leader without an authenticated user in the security context.
    for (int i = 0; i < TXS; i++) {
      final int seq = i;
      assertThatCode(() -> followerDb.command("sql", "INSERT INTO " + REPLICA_TYPE + " SET seq = ?, name = 'embedded-write'", seq))
          .as("Embedded DML on follower %d should be forwarded to the leader", fIdx)
          .doesNotThrowAnyException();
    }

    LogManager.instance().log(this, Level.INFO, "Issued %d embedded inserts via follower, waiting for replication", TXS);

    // Wait for all nodes to catch up
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify all nodes see the same count
    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      final long[] count = { 0 };
      serverDb.transaction(() -> count[0] = serverDb.countType(REPLICA_TYPE, true));
      assertThat(count[0]).as("Server %d should have %d vertices of type %s", i, TXS, REPLICA_TYPE)
          .isEqualTo(TXS);
    }
  }
}
