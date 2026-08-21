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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6444 (follow-up from #6417/#6317): {@code AbstractAlgoProcedure.GraphData.adjacency()}'s CSR fallback
 * branch - taken when a {@link GraphTraversalProvider} has no {@link com.arcadedb.graph.NeighborView} to size the
 * copy from - used to checkpoint the budget every 1024 <em>nodes</em>. A single supernode's neighbour list is
 * fully materialised by {@code getNeighborIds()} in one call, so a node-count interval gives the budget no chance
 * to refuse the call until up to 1023 more nodes have also been expanded, even though the supernode alone already
 * blew the budget.
 * <p>
 * The fix checkpoints on entries-seen-so-far as well as nodes-seen-so-far, so a single oversized row is priced
 * (and can be refused) the moment it lands, rather than only after the next node-count boundary.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6444AlgoAdjacencyEntryCheckpointTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6444-adjacency-entry-checkpoint");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      GraphTraversalProviderRegistry.clearAll(database);
      database.drop();
      database = null;
    }
  }

  @Test
  void aSupernodeRowIsCheckpointedRightAwayRatherThanAfter1024Nodes() {
    // 5000 nodes so the old 1024-node interval has room to fire at least once before the loop ends, node 0 is a
    // supernode whose row alone costs far more than the budget, and every other node is empty - so an entries-
    // based checkpoint refuses right after node 0, while a nodes-based one only refuses at node 1023.
    final int nodeCount = 5000;
    final int supernodeDegree = 2_000_000;
    final SupernodeProvider provider = new SupernodeProvider(nodeCount, supernodeDegree);
    GraphTraversalProviderRegistry.register(database, provider);

    // Admits the upfront row-header reservation (5000 * 32 = 160,000 bytes) but not the supernode row on top of
    // it (2,000,000 entries * 4 bytes = 8,000,000 bytes).
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500_000L);

    assertThatThrownBy(() -> drain("CALL algo.wcc() YIELD node, componentId RETURN node"))
        .as("the supernode row alone must be enough to refuse the call")
        .hasStackTraceContaining("the adjacency list would need");

    assertThat(provider.calls.get())
        .as("a checkpoint keyed on entries-seen-so-far refuses right after the oversized row, "
            + "not after another 1023 nodes were expanded to reach the next node-count boundary")
        .isLessThan(10);
  }

  @Test
  void aBudgetThatFitsTheSupernodeLetsTheCallThrough() {
    // The counterweight: a budget generous enough for the supernode must not be refused by the new checkpoint.
    final int nodeCount = 50;
    final int supernodeDegree = 100;
    final SupernodeProvider provider = new SupernodeProvider(nodeCount, supernodeDegree);
    GraphTraversalProviderRegistry.register(database, provider);

    assertThat(drain("CALL algo.wcc() YIELD node, componentId RETURN node")).hasSize(nodeCount);
    assertThat(provider.calls.get()).isEqualTo(nodeCount);
  }

  private java.util.List<Object> drain(final String query) {
    final java.util.List<Object> rows = new java.util.ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  /**
   * A minimal {@link GraphTraversalProvider} test double: no {@link com.arcadedb.graph.NeighborView}
   * (so {@code adjacency()} takes the fallback branch under test), node 0 returns an oversized neighbour
   * array and every other node returns an empty one.
   */
  private static final class SupernodeProvider implements GraphTraversalProvider {
    private final int nodeCount;
    private final int supernodeDegree;
    final AtomicInteger calls = new AtomicInteger();

    SupernodeProvider(final int nodeCount, final int supernodeDegree) {
      this.nodeCount = nodeCount;
      this.supernodeDegree = supernodeDegree;
    }

    @Override
    public int getNodeCount() {
      return nodeCount;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public String getName() {
      return "test-supernode-provider";
    }

    @Override
    public boolean coversVertexType(final String typeName) {
      return true;
    }

    @Override
    public boolean coversEdgeType(final String edgeTypeName) {
      return true;
    }

    @Override
    public int getNodeId(final RID rid) {
      return -1;
    }

    @Override
    public RID getRID(final int nodeId) {
      return new RID(0, nodeId);
    }

    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      calls.incrementAndGet();
      return new int[nodeId == 0 ? supernodeDegree : 0];
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return nodeId == 0 ? supernodeDegree : 0;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }
  }
}
