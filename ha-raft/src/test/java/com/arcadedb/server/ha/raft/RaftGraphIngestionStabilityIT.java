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
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub discussion #3810: HA cluster instability during graph data ingestion.
 * <p>
 * Simulates a 3-node cluster ingesting vertices and edges in multiple transactions to verify
 * that the Ratis-based HA does not suffer from the reconnection loops and message number jumps
 * that affected the old custom replication implementation.
 */
@Tag("IntegrationTest")
class RaftGraphIngestionStabilityIT extends BaseRaftHATest {

  private static final int VERTEX_COUNT    = 1000;
  private static final int EDGES_PER_BATCH = 500;
  private static final int TX_BATCH_SIZE   = 200;

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
  void graphIngestionRemainsStableUnderLoad() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create schema: vertex and edge types
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Node"))
        leaderDb.getSchema().createVertexType("Node");
      if (!leaderDb.getSchema().existsType("Link"))
        leaderDb.getSchema().createEdgeType("Link");
    });

    assertClusterConsistency();

    // Phase 1: Ingest vertices in multiple transactions (simulating batched ingestion)
    LogManager.instance().log(this, Level.INFO, "TEST: Ingesting %d vertices in batches of %d...", VERTEX_COUNT, TX_BATCH_SIZE);

    for (int batch = 0; batch < VERTEX_COUNT / TX_BATCH_SIZE; batch++) {
      final int batchStart = batch * TX_BATCH_SIZE;
      leaderDb.transaction(() -> {
        for (int i = 0; i < TX_BATCH_SIZE; i++) {
          final MutableVertex v = leaderDb.newVertex("Node");
          v.set("idx", batchStart + i);
          v.set("name", "node-" + (batchStart + i));
          v.save();
        }
      });
    }

    assertClusterConsistency();

    // Verify all nodes have the vertices
    for (int i = 0; i < getServerCount(); i++) {
      final long count = getServerDatabase(i, getDatabaseName()).countType("Node", true);
      assertThat(count).as("Server %d should have %d Node vertices", i, VERTEX_COUNT).isEqualTo(VERTEX_COUNT);
    }

    // Phase 2: Create edges between vertices in batches
    // This is the critical part - the original issue had ConcurrentModificationException on edge bucket pages
    LogManager.instance().log(this, Level.INFO, "TEST: Creating edges between vertices...");

    final List<Vertex> vertices = new ArrayList<>();
    leaderDb.transaction(() -> {
      final Iterator<Vertex> iter = leaderDb.iterateType("Node", true);
      while (iter.hasNext())
        vertices.add(iter.next());
    });

    int edgesCreated = 0;
    for (int batch = 0; batch < vertices.size() / EDGES_PER_BATCH; batch++) {
      final int batchStart = batch * EDGES_PER_BATCH;
      final int batchEnd = Math.min(batchStart + EDGES_PER_BATCH, vertices.size() - 1);
      leaderDb.transaction(() -> {
        for (int i = batchStart; i < batchEnd; i++) {
          final Vertex from = vertices.get(i).asVertex();
          final Vertex to = vertices.get(i + 1).asVertex();
          from.asVertex().newEdge("Link", to, "weight", i);
        }
      });
      edgesCreated += (batchEnd - batchStart);
    }

    final int totalEdges = edgesCreated;
    LogManager.instance().log(this, Level.INFO, "TEST: Created %d edges total", totalEdges);

    assertClusterConsistency();

    // Phase 3: Verify all servers are stable and have consistent data
    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      final long vertexCount = nodeDb.countType("Node", true);
      final long edgeCount = nodeDb.countType("Link", true);

      assertThat(vertexCount).as("Server %d should have %d vertices", i, VERTEX_COUNT).isEqualTo(VERTEX_COUNT);
      assertThat(edgeCount).as("Server %d should have %d edges", i, totalEdges).isEqualTo(totalEdges);
    }

    // Phase 4: Verify all servers are still connected and operational
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).isStarted()).as("Server %d should still be running", i).isTrue();

    // Full database comparison
    checkDatabasesAreIdentical();
  }

  @Test
  void graphIngestionWithReplicaCrashRemainsStable() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Pick a follower to crash mid-ingestion
    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    }
    assertThat(replicaIndex).as("Must find a replica").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create schema
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Controller"))
        leaderDb.getSchema().createVertexType("Controller");
      if (!leaderDb.getSchema().existsType("Connection"))
        leaderDb.getSchema().createEdgeType("Connection");
    });

    assertClusterConsistency();

    // Phase 1: Ingest initial data with all 3 nodes
    leaderDb.transaction(() -> {
      for (int i = 0; i < 500; i++) {
        final MutableVertex v = leaderDb.newVertex("Controller");
        v.set("idx", i);
        v.set("name", "controller-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Phase 2: Crash one replica, then continue ingesting - this is the scenario from the issue
    // where edge page contention caused ConcurrentModificationException and infinite reconnection
    LogManager.instance().log(this, Level.INFO, "TEST: Crashing replica %d during graph ingestion", replicaIndex);
    getServer(replicaIndex).stop();

    // Create edges while replica is down (leader + 1 follower = majority 2/3)
    final List<Vertex> vertices = new ArrayList<>();
    leaderDb.transaction(() -> {
      final Iterator<Vertex> iter = leaderDb.iterateType("Controller", true);
      while (iter.hasNext())
        vertices.add(iter.next());
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < vertices.size() - 1; i++) {
        final Vertex from = vertices.get(i).asVertex();
        final Vertex to = vertices.get(i + 1).asVertex();
        from.asVertex().newEdge("Connection", to, "seq", i);
      }
    });

    // Add more vertices while replica is still down
    leaderDb.transaction(() -> {
      for (int i = 500; i < 1000; i++) {
        final MutableVertex v = leaderDb.newVertex("Controller");
        v.set("idx", i);
        v.set("name", "controller-" + i);
        v.save();
      }
    });

    // Phase 3: Restart the crashed replica - it should catch up via Raft log replay
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d", replicaIndex);
    try {
      Thread.sleep(2_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    getServer(replicaIndex).start();
    waitForReplicationIsCompleted(replicaIndex);

    // Phase 4: Verify the recovered replica has all data
    final long expectedVertices = 1000;
    final long expectedEdges = 499;

    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType("Controller", true))
          .as("Server %d should have %d vertices", i, expectedVertices).isEqualTo(expectedVertices);
      assertThat(nodeDb.countType("Connection", true))
          .as("Server %d should have %d edges", i, expectedEdges).isEqualTo(expectedEdges);
    }

    // Full database comparison
    assertClusterConsistency();
    checkDatabasesAreIdentical();
  }
}
