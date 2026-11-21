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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12017: with a Graph Analytical View over the walked types, the strategy swaps the vertex steps for view steps when the
 * traversal is compiled. A traversal that writes an edge and then walks that edge type read the view, which serves the committed
 * graph, so it missed its own write (or still walked an edge it had dropped). The view steps now walk the records while the
 * transaction holds uncommitted changes, as a traversal compiled after the write already does.
 */
class Issue12017GremlinGAVOwnWritesTest {
  private ArcadeGraph         graph;
  private GraphAnalyticalView gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/testgremlin-gav-12017");
    graph.getDatabase().getSchema().createVertexType("P");
    graph.getDatabase().getSchema().createEdgeType("K");

    graph.getDatabase().transaction(() -> {
      final MutableVertex v1 = graph.getDatabase().newVertex("P").set("n", 1).save();
      final MutableVertex v2 = graph.getDatabase().newVertex("P").set("n", 2).save();
      graph.getDatabase().newVertex("P").set("n", 3).save();
      v1.newEdge("K", v2);
    });

    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("P")
        .withEdgeTypes("K")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
  }

  @AfterEach
  void teardown() {
    if (gav != null)
      gav.drop();
    if (graph != null)
      graph.drop();
  }

  private Object single(final String gremlin) {
    try (final ResultSet rs = graph.gremlin(gremlin).execute()) {
      return rs.next().getProperty("result");
    }
  }

  @Test
  void theViewStepIsInTheWritingTraversal() {
    // THE TESTS BELOW WOULD PASS ON THE RECORDS ALONE: THE PLAN MUST STILL HOLD THE VIEW STEP FOR THEM TO MEAN ANYTHING
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("P", "n", 1).sideEffect(addE("K").to(graph.traversal().V().has("P", "n", 3).next())).out("K"),
        ArcadeGAVVertexStep.class)).isTrue();
  }

  @Test
  void anEdgeAddedEarlierInTheTraversalIsWalked() {
    assertThat(single("g.V().has('P','n',1).sideEffect(__.addE('K').to(__.V().has('P','n',3))).out('K').values('n').order().fold()"))
        .isEqualTo(List.of(2, 3));
    // THE NEXT TRAVERSAL READS THE COMMITTED GRAPH THROUGH THE VIEW AND AGREES
    assertThat(single("g.V().has('P','n',1).out('K').values('n').order().fold()")).isEqualTo(List.of(2, 3));
  }

  @Test
  void anEdgeDroppedEarlierInTheTraversalIsNotWalked() {
    assertThat(single("g.V().has('P','n',1).sideEffect(__.outE('K').drop()).out('K').values('n').order().fold()"))
        .isEqualTo(List.of());
    assertThat(single("g.V().has('P','n',1).out('K').values('n').order().fold()")).isEqualTo(List.of());
  }

  @Test
  void aMidTraversalHopSeesTheWrite() {
    assertThat(single(
        "g.V().has('P','n',1).sideEffect(__.addE('K').to(__.V().has('P','n',3))).V().has('P','n',3).in('K').values('n').fold()"))
        .isEqualTo(List.of(1));
  }

  @Test
  void aFusedChainSeesTheWrite() {
    // 3 -K-> 1 -K-> 2: TWO CONSECUTIVE HOPS ARE FUSED INTO ONE VIEW STEP
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().has("P", "n", 3).out("K").out("K"), ArcadeGAVFusedStep.class))
        .isTrue();
    assertThat(single("g.V().has('P','n',3).sideEffect(__.addE('K').to(__.V().has('P','n',1))).out('K').out('K').values('n').fold()"))
        .isEqualTo(List.of(2));
  }

  @Test
  void theFluentApiInOneTransactionSeesTheWrite() {
    graph.tx().open();
    try {
      final List<Object> found = graph.traversal().V().has("P", "n", 1).sideEffect(outE("K").drop()).out("K").values("n").toList();
      assertThat(found).isEmpty();
    } finally {
      graph.tx().rollback();
    }
    assertThat(single("g.V().has('P','n',1).out('K').values('n').fold()")).isEqualTo(List.of(2));
  }
}
