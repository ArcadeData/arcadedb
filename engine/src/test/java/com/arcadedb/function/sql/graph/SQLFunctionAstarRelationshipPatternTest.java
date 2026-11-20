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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.RelationshipPattern;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The A* search the Cypher {@code algo.*} path finders run, fed an APOC relationship pattern whose alternatives mix
 * directions (issue #12103), and the weight it gives an edge without the weight property.
 * <p>
 * The graph: {@code (a)-[:KNOWS]->(b)}, {@code (c)-[:LIKES]->(b)}, and two shortcuts the pattern {@code 'KNOWS>|<LIKES'} does
 * not follow from {@code a}: {@code (a)-[:LIKES]->(c)} (LIKES outgoing) and {@code (c)-[:KNOWS]->(a)} (KNOWS incoming).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionAstarRelationshipPatternTest extends TestHelper {
  private MutableVertex a;
  private MutableVertex b;
  private MutableVertex c;

  @BeforeEach
  void createGraph() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("LIKES");
    database.transaction(() -> {
      a = database.newVertex("P").set("name", "a").save();
      b = database.newVertex("P").set("name", "b").save();
      c = database.newVertex("P").set("name", "c").save();
      a.newEdge("KNOWS", b, "w", 1.0).save();
      c.newEdge("LIKES", b, "w", 1.0).save();
      a.newEdge("LIKES", c, "w", 1.0).save();
      c.newEdge("KNOWS", a, "w", 1.0).save();
    });
  }

  @Test
  void mixedPatternWalksEachAlternativeInItsOwnDirection() {
    // control: every relationship both ways takes a shortcut
    assertThat(search(null)).containsExactly(a.getIdentity(), c.getIdentity());
    // KNOWS outgoing to b, then LIKES incoming to c
    assertThat(search(RelationshipPattern.parse("test", "KNOWS>|<LIKES"))).containsExactly(a.getIdentity(), b.getIdentity(),
        c.getIdentity());
    // neither alternative leaves a toward c the other way round
    assertThat(search(RelationshipPattern.parse("test", "<KNOWS|LIKES>"))).containsExactly(a.getIdentity(), c.getIdentity());
  }

  @Test
  void mixedPatternDoesNotTakeTheGraphAnalyticalView() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("astar-pattern-view")
        .withVertexTypes("P")
        .withEdgeTypes("KNOWS", "LIKES")
        .withEdgeProperties("w")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    try {
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "KNOWS", "LIKES"))
          .as("the view must cover the pattern's types, or this test does not reach the provider lookup the pattern skips")
          .isNotNull();
      // the view answers one direction and one type list per node, which cannot say 'KNOWS>|<LIKES'
      assertThat(search(RelationshipPattern.parse("test", "KNOWS>|<LIKES"))).containsExactly(a.getIdentity(), b.getIdentity(),
          c.getIdentity());
    } finally {
      view.drop();
    }
  }

  @Test
  void anEdgeWithoutTheWeightPropertyWeighsWhatItAlwaysDid() {
    final Edge[] edges = new Edge[3];
    database.transaction(() -> {
      edges[0] = b.newEdge("KNOWS", c).save();
      edges[1] = b.newEdge("KNOWS", c, "weight", "heavy").save();
      edges[2] = b.newEdge("KNOWS", c, "weight", 3.0).save();
    });

    // the default rule is unchanged: EdgeWeight.of(null) was MISSING, and a missing property or edge still weighs MISSING
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    assertThat(EdgeWeight.of(null)).isEqualTo(EdgeWeight.MISSING);
    assertThat(astar.getDistance(edges[0])).isEqualTo(EdgeWeight.MISSING);
    assertThat(astar.getDistance(null)).isEqualTo(EdgeWeight.MISSING);
    assertThat(astar.getDistance(edges[1])).isEqualTo(EdgeWeight.of("heavy"));
    assertThat(astar.getDistance(edges[2])).isEqualTo(3.0);

    // APOC's defaultWeight replaces the weight of an edge without a numeric weight, the one every reader of the weight
    // column of a graph analytical view gives it too (see aCustomDefaultWeightWeighsANonNumericWeightTheSameWithAndWithoutAView)
    final SQLFunctionAstar withDefault = new SQLFunctionAstar().setMissingWeight(2.5);
    assertThat(withDefault.getDistance(edges[0])).isEqualTo(2.5);
    assertThat(withDefault.getDistance(null)).isEqualTo(2.5);
    assertThat(withDefault.getDistance(edges[1])).isEqualTo(2.5);
    assertThat(withDefault.getDistance(edges[2])).isEqualTo(3.0);
  }

  @Test
  void aCustomDefaultWeightWeighsANonNumericWeightTheSameWithAndWithoutAView() {
    // end0 reaches end2 by one edge whose weight is not a number, or by two edges of weight 1: with a default weight of 5 the
    // two edges are the cheaper way, with the default of 1 the single edge is
    database.getSchema().createEdgeType("TEXTUAL");
    final MutableVertex[] ends = new MutableVertex[3];
    database.transaction(() -> {
      for (int i = 0; i < ends.length; i++)
        ends[i] = database.newVertex("P").set("name", "end" + i).save();
      ends[0].newEdge("TEXTUAL", ends[2], "w", "n/a").save();
      ends[0].newEdge("KNOWS", ends[1], "w", 1.0).save();
      ends[1].newEdge("KNOWS", ends[2], "w", 1.0).save();
    });

    final List<RID> direct = List.of(ends[0].getIdentity(), ends[2].getIdentity());
    final List<RID> viaMiddle = List.of(ends[0].getIdentity(), ends[1].getIdentity(), ends[2].getIdentity());

    assertThat(searchEnds(ends, null)).as("OLTP, the default weight of 1").isEqualTo(direct);
    assertThat(searchEnds(ends, 5.0)).as("OLTP, a default weight of 5").isEqualTo(viaMiddle);

    // the view types a weight column from the schema, and leaves the slot of a value that is not a number empty: the default
    database.getSchema().getType("KNOWS").createProperty("w", Type.DOUBLE);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("astar-default-weight-view")
        .withVertexTypes("P")
        .withEdgeTypes("KNOWS", "TEXTUAL")
        .withEdgeProperties("w")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    try {
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(database, "KNOWS", "TEXTUAL");
      assertThat(provider).as("the view must cover both types, or this test does not reach the weights the view serves").isNotNull();
      final NodeEdgeWeights served = provider.edgeWeightsOf(provider.getNodeId(ends[0].getIdentity()), Vertex.DIRECTION.OUT, "w", 5.0,
          "KNOWS", "TEXTUAL");
      assertThat(served).as("the view serves the weights, or the half below would only repeat the OLTP one").isNotNull();
      assertThat(served.weights()).as("the view weighs the text weight with the default").containsExactlyInAnyOrder(5.0, 1.0);
      assertThat(searchEnds(ends, null)).as("view, the default weight of 1").isEqualTo(direct);
      assertThat(searchEnds(ends, 5.0)).as("view, a default weight of 5").isEqualTo(viaMiddle);
    } finally {
      view.drop();
    }
  }

  private List<RID> searchEnds(final MutableVertex[] ends, final Double defaultWeight) {
    final Map<String, Object> options = new HashMap<>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "OUT");
    options.put("emptyIfMaxDepth", true);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    if (defaultWeight != null)
      astar.setMissingWeight(defaultWeight);
    return astar.execute(null, null, null, new Object[] { ends[0], ends[2], "w", options }, context);
  }

  private List<RID> search(final RelationshipPattern pattern) {
    final Map<String, Object> options = new HashMap<>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "BOTH");
    options.put("emptyIfMaxDepth", true);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    if (pattern != null)
      astar.setRelationshipPattern(pattern);
    return astar.execute(null, null, null, new Object[] { a, c, "w", options }, context);
  }
}
