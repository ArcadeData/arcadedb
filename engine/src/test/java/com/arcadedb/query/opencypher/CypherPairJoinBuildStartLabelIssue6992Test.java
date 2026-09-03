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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression coverage for issue #6992: CSR pair joins must restrict build anchors to their declared label. */
class CypherPairJoinBuildStartLabelIssue6992Test extends TestHelper {
  private static final String MATCH =
      "MATCH (left)-[:PROBE]->(right), (left)<-[:ARM_1]-(build:Build)-[:ARM_2]->(right)";
  private static final String TWO_HOP_MATCH =
      "MATCH (left)-[:PROBE]->(right), " +
          "(left)<-[:ARM_1]-(build:Build)-[:ARM_2_FIRST]->(middle)-[:ARM_2_SECOND]->(right)";

  private RID outsideBuildRid;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Build");
      database.getSchema().createVertexType("Other");
      database.getSchema().createVertexType("Endpoint");
      database.getSchema().createVertexType("Intermediate");
      database.getSchema().createEdgeType("ARM_1");
      database.getSchema().createEdgeType("ARM_2");
      database.getSchema().createEdgeType("ARM_2_FIRST");
      database.getSchema().createEdgeType("ARM_2_SECOND");
      database.getSchema().createEdgeType("PROBE");

      final MutableVertex build = database.newVertex("Build").save();
      final MutableVertex outsideBuild = database.newVertex("Other").save();
      final MutableVertex left = database.newVertex("Endpoint").save();
      final MutableVertex right = database.newVertex("Endpoint").save();
      final MutableVertex middle = database.newVertex("Intermediate").save();

      build.newEdge("ARM_1", left).save();
      build.newEdge("ARM_2", right).save();
      outsideBuild.newEdge("ARM_1", left).save();
      outsideBuild.newEdge("ARM_2", right).save();
      build.newEdge("ARM_2_FIRST", middle).save();
      outsideBuild.newEdge("ARM_2_FIRST", middle).save();
      middle.newEdge("ARM_2_SECOND", right).save();
      left.newEdge("PROBE", right).save();

      outsideBuildRid = outsideBuild.getIdentity();
    });
  }

  @Test
  void buildStartLabelExcludesOtherLiveTypesWhenAllCsrViewsAreAvailable() {
    assertCountWithAllViews(MATCH);
  }

  @Test
  void buildStartLabelExcludesOtherLiveTypesOnTheFusedTwoHopPath() {
    assertCountWithAllViews(TWO_HOP_MATCH);
  }

  private void assertCountWithAllViews(final String matchClause) {
    assertThat(explainOf(matchClause + " RETURN count(*) AS c")).contains("COUNT PAIR JOIN");
    assertThat(rowCountOf(matchClause + " RETURN build")).as("materialized pipeline").isEqualTo(1);
    assertThat(scalarOf(matchClause + " RETURN count(*) AS c")).as("OLTP").isEqualTo(1L);

    final GraphAnalyticalView view = newViewOverFixture();
    try {
      final int outsideBuildId = view.getNodeId(outsideBuildRid);
      assertThat(outsideBuildId).isGreaterThanOrEqualTo(0);
      assertThat(view.isNodeLive(outsideBuildId)).isTrue();
      assertThat(scalarOf(matchClause + " RETURN count(*) AS c")).as("CSR").isEqualTo(1L);
    } finally {
      view.drop();
    }
  }

  @Test
  void buildStartLabelExcludesOtherLiveTypesWhenIndividualCsrViewsAreUnavailable() {
    assertThat(rowCountOf(MATCH + " RETURN build")).as("materialized pipeline").isEqualTo(1);
    assertThat(scalarOf(MATCH + " RETURN count(*) AS c")).as("OLTP").isEqualTo(1L);

    assertCountWithHiddenView("PROBE");
    assertCountWithHiddenView("ARM_2");
    assertCountWithHiddenView("ARM_1");
  }

  private void assertCountWithHiddenView(final String hiddenEdgeType) {
    final GraphAnalyticalView view = newViewOverFixture();
    final GraphTraversalProvider hiddenView = new ViewHidingProvider(view, hiddenEdgeType);
    try {
      GraphTraversalProviderRegistry.unregister(database, view);
      GraphTraversalProviderRegistry.register(database, hiddenView);
      assertThat(scalarOf(MATCH + " RETURN count(*) AS c"))
          .as("CSR without %s NeighborView", hiddenEdgeType)
          .isEqualTo(1L);
    } finally {
      GraphTraversalProviderRegistry.unregister(database, hiddenView);
      GraphTraversalProviderRegistry.register(database, view);
      view.drop();
    }
  }

  private GraphAnalyticalView newViewOverFixture() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("issue-6992")
        .withVertexTypes("Build", "Other", "Endpoint", "Intermediate")
        .withEdgeTypes("ARM_1", "ARM_2", "ARM_2_FIRST", "ARM_2_SECOND", "PROBE")
        .build();
    assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
    assertThat(GraphTraversalProviderRegistry.findProvider(database,
        "ARM_1", "ARM_2", "ARM_2_FIRST", "ARM_2_SECOND", "PROBE")).isNotNull();
    return view;
  }

  private long scalarOf(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
    }
    assertThat(values).as(query).hasSize(1);
    return values.get(0);
  }

  private int rowCountOf(final String query) {
    int count = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** Keeps all graph data available while making one zero-copy adjacency view unavailable. */
  private record ViewHidingProvider(GraphAnalyticalView delegate, String hiddenEdgeType)
      implements GraphTraversalProvider {

    @Override
    public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
      if (edgeTypes != null)
        for (final String edgeType : edgeTypes)
          if (hiddenEdgeType.equals(edgeType))
            return null;
      return delegate.getNeighborView(direction, edgeTypes);
    }

    @Override
    public int getNodeCount() {
      return delegate.getNodeCount();
    }

    @Override
    public int getNodeIdUpperBound() {
      return delegate.getNodeIdUpperBound();
    }

    @Override
    public boolean isNodeLive(final int nodeId) {
      return delegate.isNodeLive(nodeId);
    }

    @Override
    public boolean isReady() {
      return delegate.isReady();
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public boolean coversVertexType(final String typeName) {
      return delegate.coversVertexType(typeName);
    }

    @Override
    public boolean coversEdgeType(final String edgeTypeName) {
      return delegate.coversEdgeType(edgeTypeName);
    }

    @Override
    public int getNodeId(final RID rid) {
      return delegate.getNodeId(rid);
    }

    @Override
    public RID getRID(final int nodeId) {
      return delegate.getRID(nodeId);
    }

    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return delegate.getNeighborIds(nodeId, direction, edgeTypes);
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return delegate.countEdges(nodeId, direction, edgeTypes);
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
        final String... edgeTypes) {
      return delegate.isConnectedTo(nodeA, nodeB, direction, edgeTypes);
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return delegate.getProperty(nodeId, propertyName);
    }
  }
}
