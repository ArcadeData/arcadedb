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

import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10196: an element dropped earlier in a traversal stayed usable. {@code addE} from a dropped vertex
 * committed an edge whose source no longer existed (and, when the vertex had edges, failed with a misleading "concurrent commit in
 * flight"), {@code valueMap} still answered the old properties, {@code inV()} walked from a dropped edge and {@code property()}
 * on a dropped vertex was accepted. TinkerGraph refuses the writes ("Vertex with id N was removed") and answers the reads with
 * nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10196DroppedElementUseTest {
  private static final String PATH = "./target/test-gremlin-10196-dropped-use";

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
  }

  @AfterEach
  void teardown() {
    if (graph != null) {
      if (graph.tx().isOpen())
        graph.tx().rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
  }

  private void seed(final boolean withEdge) {
    final Vertex v1 = graph.addVertex(T.label, "n", "uid", 1);
    final Vertex v2 = graph.addVertex(T.label, "n", "uid", 2);
    if (withEdge)
      v1.addEdge("e", v2, "uid", 101);
    graph.tx().commit();
  }

  private String state() {
    final GraphTraversalSource g = graph.traversal();
    final List<Object> vertices = g.V().values("uid").order().toList();
    final List<String> edges = new ArrayList<>();
    for (final Edge e : g.E().toList())
      edges.add(e.label() + " " + e.outVertex().value("uid") + "->" + e.inVertex().value("uid"));
    Collections.sort(edges);
    return "vertices " + vertices + ", edges " + edges;
  }

  @Test
  void addEdgeFromADroppedVertexIsRefused() {
    seed(false);
    final GraphTraversalSource g = graph.traversal();

    assertThatThrownBy(() -> g.V().has("uid", 1).sideEffect(__.drop()).addE("r").to(__.V().has("uid", 2)).toList())//
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    graph.tx().rollback();

    // NOTHING WAS WRITTEN: THE ROLLED BACK DROP LEAVES BOTH VERTICES, AND NO EDGE WAS EVER STORED
    assertThat(state()).isEqualTo("vertices [1, 2], edges []");
  }

  @Test
  void addEdgeFromADroppedVertexWithEdgesIsRefusedAsRemovedNotAsAConflict() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    assertThatThrownBy(() -> g.V().has("uid", 1).sideEffect(__.drop()).addE("r").to(__.V().has("uid", 2)).toList())//
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    graph.tx().rollback();
    assertThat(state()).isEqualTo("vertices [1, 2], edges [e 1->2]");
  }

  @Test
  void addEdgeToADroppedVertexIsRefused() {
    seed(false);
    final GraphTraversalSource g = graph.traversal();

    assertThatThrownBy(() -> g.V().has("uid", 2).sideEffect(__.drop()).addE("r").from(__.V().has("uid", 1)).toList())//
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    graph.tx().rollback();
    assertThat(state()).isEqualTo("vertices [1, 2], edges []");
  }

  @Test
  void aVertexDroppedThroughAnotherInstanceIsRefusedByTheEngine() {
    seed(false);
    final GraphTraversalSource g = graph.traversal();
    final Vertex held = g.V().has("uid", 1).next();
    final Vertex target = g.V().has("uid", 2).next();

    // DROPPED THROUGH A DIFFERENT INSTANCE: THE HELD ONE IS NOT FLAGGED, THE TRANSACTION KNOWS
    g.V().has("uid", 1).drop().iterate();
    assertThatThrownBy(() -> held.addEdge("r", target)).isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    assertThatThrownBy(() -> target.addEdge("r", held)).isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");

    // AND BELOW GREMLIN, THE ENGINE REFUSES THE EDGE ITSELF, BEFORE ANYTHING IS STORED
    final com.arcadedb.graph.Vertex heldRecord = ((ArcadeVertex) held).getBaseElement();
    final com.arcadedb.graph.Vertex targetRecord = ((ArcadeVertex) target).getBaseElement();
    assertThatThrownBy(() -> heldRecord.newEdge("r", targetRecord)).isInstanceOf(VertexNotFoundException.class)
        .hasMessageContaining("deleted in this transaction");
    assertThatThrownBy(() -> targetRecord.newEdge("r", heldRecord)).isInstanceOf(VertexNotFoundException.class)
        .hasMessageContaining("deleted in this transaction");

    graph.tx().commit();
    assertThat(state()).isEqualTo("vertices [2], edges []");
  }

  @Test
  void valueMapOfADroppedVertexIsEmpty() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    final List<?> result = g.V().has("uid", 1).sideEffect(__.drop()).valueMap("uid").toList();
    assertThat(result).hasSize(1);
    assertThat((Map<?, ?>) result.getFirst()).isEmpty();
    assertThat(g.V().has("uid", 2).sideEffect(__.drop()).values("uid").toList()).isEmpty();
    graph.tx().commit();
    assertThat(state()).isEqualTo("vertices [], edges []");
  }

  @Test
  void aDroppedEdgeLeadsNowhere() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    assertThat(g.E().has("uid", 101).sideEffect(__.drop()).inV().values("uid").toList()).isEmpty();
    graph.tx().commit();
    assertThat(state()).isEqualTo("vertices [1, 2], edges []");
  }

  @Test
  void aDroppedVertexHasNoEdgesAndNoNeighbours() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    assertThat(g.V().has("uid", 1).sideEffect(__.drop()).out().toList()).isEmpty();
    graph.tx().rollback();
    assertThat(g.V().has("uid", 1).sideEffect(__.drop()).outE().toList()).isEmpty();
    graph.tx().commit();
    assertThat(state()).isEqualTo("vertices [2], edges []");
  }

  @Test
  void propertyOnADroppedVertexIsRefused() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    assertThatThrownBy(() -> g.V().has("uid", 1).sideEffect(__.drop()).property("k", 1).toList())//
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    graph.tx().rollback();
    assertThat(state()).isEqualTo("vertices [1, 2], edges [e 1->2]");
  }

  @Test
  void propertyOnADroppedEdgeIsRefused() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    assertThatThrownBy(() -> g.E().has("uid", 101).sideEffect(__.drop()).property("k", 1).toList())//
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    graph.tx().rollback();
    assertThat(state()).isEqualTo("vertices [1, 2], edges [e 1->2]");
  }

  @Test
  void anEdgeTakenByItsVertexDeleteIsRefusedOnWrite() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();
    final Edge edge = g.E().has("uid", 101).next();

    g.V().has("uid", 1).drop().iterate();
    assertThatThrownBy(() -> edge.property("k", 1)).isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    graph.tx().commit();
    assertThat(state()).isEqualTo("vertices [2], edges []");
  }

  @Test
  void aRolledBackDropLeavesTheElementUsable() {
    seed(true);
    final Vertex v1 = graph.traversal().V().has("uid", 1).next();
    final Vertex v2 = graph.traversal().V().has("uid", 2).next();

    v1.remove();
    assertThat(v1.keys()).isEmpty();
    graph.tx().rollback();

    // THE DELETE WAS TAKEN BACK: THE SAME INSTANCE READS AND WRITES AGAIN
    assertThat((Integer) v1.value("uid")).isEqualTo(1);
    v1.property("k", 7);
    v1.addEdge("r", v2);
    graph.tx().commit();
    assertThat(graph.traversal().V().has("uid", 1).values("k").toList()).containsExactly(7);
    assertThat(state()).isEqualTo("vertices [1, 2], edges [e 1->2, r 1->2]");
  }

  @Test
  void anotherInstanceOfADroppedVertexReadsNothing() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();
    final Vertex held = g.V().has("uid", 1).next();

    g.V().has("uid", 1).drop().iterate();
    // NOT THE INSTANCE THAT WAS DROPPED: THE TRANSACTION KNOWS
    assertThat(held.keys()).isEmpty();
    assertThat(held.properties()).toIterable().isEmpty();
    assertThat(held.edges(Direction.OUT)).toIterable().isEmpty();
    graph.tx().rollback();
    assertThat((Integer) held.value("uid")).isEqualTo(1);
  }

  @Test
  void aCommittedDropStaysDroppedWhenItsRidIsReused() {
    seed(false);
    final Vertex v1 = graph.traversal().V().has("uid", 1).next();
    final Object droppedId = v1.id();
    v1.remove();
    graph.tx().commit();

    // A NEW VERTEX MAY TAKE THE FREED SLOT: THE OLD INSTANCE MUST NOT COME BACK AS IT
    Vertex reused = null;
    for (int i = 0; i < 20 && reused == null; i++) {
      final Vertex created = graph.addVertex(T.label, "n", "uid", 100 + i);
      graph.tx().commit();
      if (created.id().equals(droppedId))
        reused = created;
    }
    assertThat(v1.keys()).isEmpty();
    assertThatThrownBy(() -> v1.property("k", 1)).isInstanceOf(IllegalStateException.class).hasMessageContaining("was removed");
    if (reused != null)
      assertThat(graph.traversal().V(droppedId).values("k").toList()).isEmpty();
  }

  @Test
  void droppingTwiceIsStillANoOp() {
    seed(true);
    final GraphTraversalSource g = graph.traversal();

    // #9145: THE SAME ELEMENT REACHED TWICE IS DROPPED ONCE, WITHOUT AN ERROR
    g.V().has("uid", 1).sideEffect(__.drop()).drop().iterate();
    graph.tx().commit();
    assertThat(state()).isEqualTo("vertices [2], edges []");
  }
}
