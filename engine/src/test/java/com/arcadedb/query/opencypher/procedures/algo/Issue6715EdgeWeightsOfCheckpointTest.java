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
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6715: {@code AbstractAlgoProcedure.GraphData.weightedAdjacencyFromColumns} checkpointed its
 * {@link com.arcadedb.query.sql.executor.WorkGuard} once per <em>node</em>, but
 * {@link GraphTraversalProvider#edgeWeightsOf} - the one call each iteration makes to read a node's weighted
 * edges from a Graph Analytical View's columns - had no checkpoint of its own inside its O(degree) loop. A
 * supernode's row was therefore one unabortable unit of work: once {@code edgeWeightsOf} was entered for it,
 * nothing could stop the call until the whole row was built, however large the degree.
 * <p>
 * The fix threads the guard into {@code edgeWeightsOf} itself (a new overload taking a per-edge checkpoint
 * callback) so the walk can be interrupted mid-row, and gives the columnar build the same cumulative memory
 * reservation {@code adjacency()} already has, closing the second gap the issue's own follow-up comment named:
 * a supernode's arrays were never charged against {@code CYPHER_ALGO_MAX_WORKING_MEMORY} at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6715EdgeWeightsOfCheckpointTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6715-edgeweightsof-checkpoint");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
    Thread.interrupted();
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      GraphTraversalProviderRegistry.clearAll(database);
      database.drop();
      database = null;
    }
  }

  /**
   * The interrupt is armed from inside the provider itself, once the walk is a handful of edges into the
   * supernode's row - not before the call, which the outer per-node checkpoint would already catch regardless of
   * this fix. Before the fix, {@code edgeWeightsOf} has no checkpoint of its own, so the row is built to
   * completion (all {@code supernodeDegree} of the supernode's edges) before the interrupt is ever observed. After the fix, the per-edge checkpoint restarts at zero for this call and
   * fires at the next multiple of 1024, so the walk stops within about one checkpoint stride of where the
   * interrupt was raised - orders of magnitude short of the full degree.
   */
  @Test
  void abortsMidSupernodeRatherThanBuildingTheWholeRow() {
    final int nodeCount = 10;
    final int supernodeIndex = 3;
    final int supernodeDegree = 5_000_000;
    final WeightedSupernodeProvider provider = new WeightedSupernodeProvider(nodeCount, supernodeIndex,
        supernodeDegree, "LINK", "w", 5);
    GraphTraversalProviderRegistry.register(database, provider);

    assertThatThrownBy(() -> drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node"))
        .as("a per-edge checkpoint inside edgeWeightsOf must observe the interrupt raised mid-row")
        .hasStackTraceContaining("algo.maxKCut() has been interrupted");

    assertThat(provider.supernodeEdgeCalls.get())
        .as("the walk must stop close to where the interrupt was raised, not after building the whole "
            + supernodeDegree + "-edge row")
        .isLessThan(2000);
  }

  /**
   * The counterweight: a healthy call - nothing armed - still reads every edge of a (much smaller) supernode and
   * returns its result, so the new checkpoint costs nothing when there is nothing to abort.
   */
  @Test
  void aHealthyCallStillReadsEveryEdgeOfTheSupernode() {
    final int nodeCount = 10;
    final int supernodeIndex = 3;
    final int supernodeDegree = 500;
    final WeightedSupernodeProvider provider = new WeightedSupernodeProvider(nodeCount, supernodeIndex,
        supernodeDegree, "LINK", "w", -1);
    GraphTraversalProviderRegistry.register(database, provider);

    assertThat(drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node")).isNotEmpty();
    assertThat(provider.supernodeEdgeCalls.get()).isEqualTo(supernodeDegree);
  }

  /**
   * The memory side of the same gap, named in the issue's own follow-up comment: before this fix, the columnar
   * weighted-adjacency build never reserved anything at all against {@code CYPHER_ALGO_MAX_WORKING_MEMORY}, so
   * many medium-sized rows could together exceed the budget with nothing to refuse the call. 3000 nodes of 2000
   * edges each is 6,000,000 entries; a tight 2,000,000-byte budget - which admits the row headers and only a
   * small fraction of the entries, since the checkpoint interval is capped by {@code MemoryBudget.capacityFor}
   * against what is actually left of it, not just the coarser {@code ADJACENCY_CHECKPOINT_ENTRIES / 3} - must
   * refuse within the first few dozen nodes, well short of building all 3000 rows.
   */
  @Test
  void manyMediumRowsTogetherExceedTheBudgetAndAreRefused() {
    final int nodeCount = 3000;
    final int degreePerNode = 2000;
    final UniformDegreeProvider provider = new UniformDegreeProvider(nodeCount, degreePerNode, "LINK", "w");
    GraphTraversalProviderRegistry.register(database, provider);

    // Admits the row-header reservation (3000 nodes * 2 arrays * 32 bytes = 192,000 bytes) but not the full
    // 6,000,000-entry payload (6,000,000 * (4 + 8) bytes = 72,000,000 bytes).
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 2_000_000L);

    assertThatThrownBy(() -> drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node"))
        .as("the cumulative cost of many medium rows must be caught, not only a single oversized one")
        .hasStackTraceContaining("the weighted adjacency");

    assertThat(provider.edgePropertyCalls.get())
        .as("the call must be refused well before every node's row was built")
        .isLessThan(nodeCount * degreePerNode);
  }

  /**
   * The counterweight: a budget generous enough for the whole graph must not be refused by the new reservation.
   */
  @Test
  void aBudgetThatFitsTheWholeGraphLetsTheCallThrough() {
    final int nodeCount = 200;
    final int degreePerNode = 50;
    final UniformDegreeProvider provider = new UniformDegreeProvider(nodeCount, degreePerNode, "LINK", "w");
    GraphTraversalProviderRegistry.register(database, provider);

    assertThat(drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node")).isNotEmpty();
    assertThat(provider.edgePropertyCalls.get()).isEqualTo(nodeCount * degreePerNode);
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  /**
   * A minimal {@link GraphTraversalProvider} test double serving one weighted edge type/property: every node is
   * empty except {@code supernodeIndex}, whose row has {@code supernodeDegree} edges all reachable through
   * {@code getEdgeProperty}. Self-interrupts the current thread once {@code getEdgeProperty} on the supernode has
   * been called {@code selfInterruptAfterCalls} times ({@code -1} disables this).
   */
  private static final class WeightedSupernodeProvider implements GraphTraversalProvider {
    private final int    nodeCount;
    private final int    supernodeIndex;
    private final int    supernodeDegree;
    private final String edgeType;
    private final String weightProperty;
    private final int    selfInterruptAfterCalls;
    final AtomicInteger supernodeEdgeCalls = new AtomicInteger();

    WeightedSupernodeProvider(final int nodeCount, final int supernodeIndex, final int supernodeDegree,
        final String edgeType, final String weightProperty, final int selfInterruptAfterCalls) {
      this.nodeCount = nodeCount;
      this.supernodeIndex = supernodeIndex;
      this.supernodeDegree = supernodeDegree;
      this.edgeType = edgeType;
      this.weightProperty = weightProperty;
      this.selfInterruptAfterCalls = selfInterruptAfterCalls;
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
      return "test-weighted-supernode-provider";
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
      return new int[nodeId == supernodeIndex ? supernodeDegree : 0];
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return nodeId == supernodeIndex ? supernodeDegree : 0;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }

    @Override
    public String[] getMaterializedEdgeTypes() {
      return new String[] { edgeType };
    }

    @Override
    public boolean hasEdgeProperties() {
      return true;
    }

    @Override
    public boolean hasEdgeProperty(final String edgeType, final String propertyName) {
      return this.edgeType.equals(edgeType) && weightProperty.equals(propertyName);
    }

    @Override
    public NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
        final String edgeType, final String propertyName, final double defaultWeight,
        final IntConsumer edgeCheckpoint) {
      final int degree = nodeId == supernodeIndex ? supernodeDegree : 0;
      final int[] neighbors = new int[degree];
      final double[] weights = new double[degree];
      for (int i = 0; i < degree; i++) {
        if (edgeCheckpoint != null)
          edgeCheckpoint.accept(i);
        final int calls = supernodeEdgeCalls.incrementAndGet();
        if (selfInterruptAfterCalls >= 0 && calls == selfInterruptAfterCalls)
          Thread.currentThread().interrupt();
        weights[i] = 1.0;
      }
      return new NodeEdgeWeights(neighbors, weights);
    }
  }

  /**
   * A {@link GraphTraversalProvider} test double where every node has the same degree, so the cost that has to
   * be refused is the sum across many nodes rather than one oversized row.
   */
  private static final class UniformDegreeProvider implements GraphTraversalProvider {
    private final int    nodeCount;
    private final int    degreePerNode;
    private final String edgeType;
    private final String weightProperty;
    final AtomicInteger edgePropertyCalls = new AtomicInteger();

    UniformDegreeProvider(final int nodeCount, final int degreePerNode, final String edgeType, final String weightProperty) {
      this.nodeCount = nodeCount;
      this.degreePerNode = degreePerNode;
      this.edgeType = edgeType;
      this.weightProperty = weightProperty;
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
      return "test-uniform-degree-provider";
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
      return new int[degreePerNode];
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return degreePerNode;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }

    @Override
    public String[] getMaterializedEdgeTypes() {
      return new String[] { edgeType };
    }

    @Override
    public boolean hasEdgeProperties() {
      return true;
    }

    @Override
    public boolean hasEdgeProperty(final String edgeType, final String propertyName) {
      return this.edgeType.equals(edgeType) && weightProperty.equals(propertyName);
    }

    @Override
    public NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
        final String edgeType, final String propertyName, final double defaultWeight,
        final IntConsumer edgeCheckpoint) {
      final int[] neighbors = new int[degreePerNode];
      final double[] weights = new double[degreePerNode];
      for (int i = 0; i < degreePerNode; i++) {
        if (edgeCheckpoint != null)
          edgeCheckpoint.accept(i);
        edgePropertyCalls.incrementAndGet();
        weights[i] = 1.0;
      }
      return new NodeEdgeWeights(neighbors, weights);
    }
  }
}
