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

/**
 * Regression tests for item 1 of issue #6304: the pair-join count push-down dropped label filters the pattern
 * had written down.
 * <p>
 * Two distinct drops, both of which make the operator answer a bigger number than the query asks for:
 * <ol>
 *   <li><b>The arm endpoint filter, on every {@code NeighborView} path.</b> The issue named
 *   {@code buildWithViews}, which ignores the {@code arm2Buckets} it is handed. That branch is real, but it is
 *   only reached when the probe edge type has no view of its own; the live case is the inline build-and-probe
 *   fast path, which reads {@code arm1Nbrs[i]} straight into the probe lookup and never applied {@code arm1}'s
 *   filter at all - because the bucket table it needs was owned by the {@code arm2} branch. The OLTP fallback
 *   applies both, so the same query answered differently depending on whether a Graph Analytical View happened
 *   to cover the edge types.</li>
 *   <li><b>The probe pattern's own node labels.</b> The two patterns share their endpoint variables, so
 *   {@code (p1:Person)-[:KNOWS]->(p2)} constrains the same variable the build pattern's arm ends on. The
 *   detector read the label off the build side only, and dropped it on <em>both</em> execution paths - so
 *   comparing CSR against OLTP would not have caught this one. Every count here is therefore cross-checked
 *   against the row count the ordinary materialization pipeline produces for the same pattern.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherPairJoinLabelFilterIssue6304Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Bot");
      database.getSchema().createVertexType("Comment");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("AUTHORED");
      database.getSchema().createEdgeType("MENTIONS");
      database.getSchema().createEdgeType("REPLY_OF");
      database.getSchema().createEdgeType("KNOWS");

      final MutableVertex p1 = database.newVertex("Person").set("k", "p1").save();
      final MutableVertex p2 = database.newVertex("Person").set("k", "p2").save();
      // The vertex that makes a dropped label visible: an author that is not a Person.
      final MutableVertex b1 = database.newVertex("Bot").set("k", "b1").save();

      final MutableVertex c1 = database.newVertex("Comment").set("k", "c1").save();
      final MutableVertex c2 = database.newVertex("Comment").set("k", "c2").save();
      final MutableVertex post = database.newVertex("Post").set("k", "post").save();

      // Two comments reaching the same mentioned Person, one authored by a Person and one by a Bot.
      c1.newEdge("AUTHORED", p1).save();
      c1.newEdge("MENTIONS", p2).save();
      c2.newEdge("AUTHORED", b1).save();
      c2.newEdge("MENTIONS", p2).save();

      // The two-hop arm of the same shape, for the fused inline path.
      c1.newEdge("REPLY_OF", post).save();
      c2.newEdge("REPLY_OF", post).save();
      post.newEdge("AUTHORED", p2).save();

      // Both authors know the mentioned Person, so the probe edge exists for both pairs.
      p1.newEdge("KNOWS", p2).save();
      b1.newEdge("KNOWS", p2).save();
    });
  }

  // ===================================================================================================
  // 1. the arm's own endpoint label
  // ===================================================================================================

  /** Both arms single-hop: the inline build-and-probe path, which is where the live drop was. */
  @Test
  void theArmEndpointLabelIsAppliedOnTheCsrPathToo() {
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)", 1);
  }

  /** The Q2 shape: a two-hop arm2 fused with the probe, with the label on the single-hop arm1. */
  @Test
  void theArmEndpointLabelIsAppliedOnTheFusedTwoHopPath() {
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:AUTHORED]->(p2)", 1);
  }

  /** And arm2's filter, which the same path did already apply, is not broken by handing arm1 one as well. */
  @Test
  void theOtherArmsEndpointLabelIsStillApplied() {
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person)", 2);
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Bot)", 0);
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Bot)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)", 1);
  }

  /**
   * Both arms filtered at once. Only c1 qualifies - its author p1 is a Person and its mentioned p2 is a Person,
   * and p1 KNOWS p2 - while c2's author b1 is a Bot.
   * <p>
   * This shape used to be cross-checked against the OLTP count alone, because the materialization pipeline
   * answered 0 for it: labelling <em>both</em> shared variables when at least one of the labels was written on
   * the second comma-separated pattern made the logical plan drop that label, and the anchor scan then named a
   * type the schema does not have. That was filed as issue #6322 and fixed, so the ordinary three-way
   * cross-check applies here like everywhere else in this class.
   */
  @Test
  void bothArmEndpointLabelsAreAppliedTogether() {
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Person)", 1);
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2:Bot)", 0);
  }

  /**
   * The branch the issue named. It is reached only when the probe edge type has no {@link NeighborView} while
   * both arms do, which no {@link GraphAnalyticalView} produces on its own - a type it covers and that carries
   * edges always has one. A provider that hides exactly that one view puts the operator on the branch with a
   * probe that still yields edges, which is what makes the dropped filter observable rather than moot.
   */
  @Test
  void theHashMapBuildPathAppliesBothFiltersToo() {
    final String pattern = "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    final long expected = rowCountOf(pattern + " RETURN c");

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("hidden-probe-view")
        .withVertexTypes("Person", "Bot", "Comment", "Post")
        .withEdgeTypes("AUTHORED", "MENTIONS", "REPLY_OF", "KNOWS")
        .build();
    try {
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();

      GraphTraversalProviderRegistry.unregister(database, view);
      final GraphTraversalProvider hidden = new ProbeViewHidingProvider(view, "KNOWS");
      GraphTraversalProviderRegistry.register(database, hidden);
      try {
        assertThat(scalarOf(pattern + " RETURN count(*) AS c")).as(pattern).isEqualTo(expected);
      } finally {
        GraphTraversalProviderRegistry.unregister(database, hidden);
        GraphTraversalProviderRegistry.register(database, view);
      }
    } finally {
      view.drop();
    }
  }

  // ===================================================================================================
  // 2. the probe pattern's own node labels
  // ===================================================================================================

  /** Written on the probe side rather than the build side, the label constrains the same variable. */
  @Test
  void aLabelOnTheProbePatternIsApplied() {
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1:Person)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)", 1);
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1:Bot)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)", 1);
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1)-[:KNOWS]->(p2:Bot), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)", 0);
  }

  /** The same label written on both sides is one filter, not a conflict. */
  @Test
  void theSameLabelOnBothSidesIsStillOneFilter() {
    assertCsrAgreesWithTheOltpPathAndThePipeline(
        "MATCH (p1:Person)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)", 1);
  }

  /**
   * Two different labels on one variable is an intersection, and the operator carries one name per hop. Rather
   * than counting whichever of the two it happened to read, the detector declines and the ordinary pipeline -
   * which evaluates both - answers the query.
   */
  @Test
  void conflictingLabelsOnOneVariableDeclineThePushDown() {
    final String pattern = "MATCH (p1:Person)-[:KNOWS]->(p2), (p1:Bot)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    assertThat(explainOf(pattern + " RETURN count(*) AS c")).doesNotContain("COUNT PAIR JOIN");
    assertCsrAgreesWithTheOltpPathAndThePipeline(pattern, 0);
  }

  /**
   * Same reasoning for a node the operator cannot express at all: {@code (a:A:B)} keeps what carries both and
   * {@code (a:A|B)} keeps what carries either, and the detector used to read the first label of the list for
   * both, which is a third thing.
   */
  @Test
  void aMultiLabelledNodeDeclinesThePushDown() {
    final String conjunction = "MATCH (p1)-[:KNOWS]->(p2), (p1:Person)<-[:AUTHORED]-(c:Comment:Post)-[:MENTIONS]->(p2)";
    assertThat(explainOf(conjunction + " RETURN count(*) AS c")).doesNotContain("COUNT PAIR JOIN");

    final String disjunction = "MATCH (p1:Person|Bot)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    assertThat(explainOf(disjunction + " RETURN count(*) AS c")).doesNotContain("COUNT PAIR JOIN");
    assertCsrAgreesWithTheOltpPathAndThePipeline(disjunction, 2);
  }

  /** The shape the operator exists for is still claimed by it. */
  @Test
  void theUnlabelledSharedVariablesAreStillPushedDown() {
    final String pattern = "MATCH (p1)-[:KNOWS]->(p2), (p1)<-[:AUTHORED]-(c:Comment)-[:MENTIONS]->(p2)";
    assertThat(explainOf(pattern + " RETURN count(*) AS c")).contains("COUNT PAIR JOIN");
    assertCsrAgreesWithTheOltpPathAndThePipeline(pattern, 2);
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  /**
   * Asserts that the pushed-down count, the same count off the CSR arrays, the row count the ordinary pipeline
   * produces for the same pattern, and the expected value all agree. The pipeline reference is what makes the
   * expected number more than a transcription of what the operator currently does - and it is the only reference
   * that catches a filter dropped by the detector, which both execution paths then share.
   */
  private void assertCsrAgreesWithTheOltpPathAndThePipeline(final String matchClause, final long expected) {
    final long pipeline = rowCountOf(matchClause + " RETURN c");
    assertThat(pipeline).as(matchClause + " (pipeline)").isEqualTo(expected);
    assertThat(scalarOf(matchClause + " RETURN count(*) AS c")).as(matchClause + " (OLTP)").isEqualTo(expected);

    final GraphAnalyticalView view = newViewOverEverything();
    try {
      assertThat(scalarOf(matchClause + " RETURN count(*) AS c")).as(matchClause + " (CSR)").isEqualTo(expected);
    } finally {
      view.drop();
    }
  }

  /**
   * A view covering every type in the fixture. Without a ready provider covering the edge types, a CSR
   * assertion would re-run the OLTP path and compare it against itself.
   */
  private GraphAnalyticalView newViewOverEverything() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("everything")
        .withVertexTypes("Person", "Bot", "Comment", "Post")
        .withEdgeTypes("AUTHORED", "MENTIONS", "REPLY_OF", "KNOWS")
        .build();
    assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
    assertThat(GraphTraversalProviderRegistry.findProvider(database, "AUTHORED", "MENTIONS", "REPLY_OF", "KNOWS"))
        .isNotNull();
    return view;
  }

  /** The single {@code c} value of a one-row query. */
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

  /** The rendered execution plan of {@code EXPLAIN <query>}. */
  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /**
   * A provider identical to the one it wraps except that one edge type has no zero-copy {@link NeighborView}.
   * The operator then falls back to the hash-map build for that shape while the probe, which goes through
   * {@code getNeighborIds}, still returns the real edges.
   */
  private record ProbeViewHidingProvider(GraphAnalyticalView delegate, String hiddenEdgeType)
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
