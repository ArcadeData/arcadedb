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

import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
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
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10308 (child of #10309): {@code ArcadeVertex.vertices()} and {@code edges()} filled a list with every
 * neighbor before returning its iterator, so an existence test ({@code filter(out(...))}) or a {@code limit()} over a supernode paid
 * for the whole adjacency list to read the first entry. The neighbors are now looked up one at a time, as the traversal pulls them.
 * <p>
 * The work is measured in records looked up (the database's own {@code existsRecord} and {@code readRecord} counters), never in wall
 * clock time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10308LazyNeighborExpansionTest {
  private static final String PATH    = "./target/test-gremlin-10308-lazy";
  // MANY SEGMENTS OF THE ADJACENCY LIST, FAR MORE RECORDS THAN ANY LOOK-UP BUDGET BELOW
  private static final int    TARGETS = 3_000;
  // THE LOOK-UPS A TRAVERSAL THAT STOPS AT THE FIRST NEIGHBORS MAY SPEND, WHATEVER THE DEGREE OF THE ROOT
  private static final long   BUDGET  = 200;

  private ArcadeGraph graph;
  private Vertex      root;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    final Schema schema = graph.getDatabase().getSchema();
    schema.createVertexType("Root");
    schema.createVertexType("Target");
    schema.createEdgeType("High");
    schema.createEdgeType("Ref");

    root = graph.addVertex(T.label, "Root", "uid", 1L);
    for (int i = 0; i < TARGETS; i++) {
      final Vertex target = graph.addVertex(T.label, "Target", "uid", (long) i);
      root.addEdge("High", target);
      if (i == TARGETS / 2)
        // A PARALLEL EDGE: THE SAME NEIGHBOR IS LISTED TWICE
        root.addEdge("High", target);
    }
    for (int i = 0; i < 3; i++)
      root.addEdge("Ref", graph.addVertex(T.label, "Target", "uid", 1_000_000L + i));
    graph.tx().commit();
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

  private long lookups() {
    final var stats = graph.getDatabase().getStats();
    return ((Number) stats.get("existsRecord")).longValue() + ((Number) stats.get("readRecord")).longValue();
  }

  @Test
  void limitOverNeighborsStopsAtTheFirstOnes() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Vertex> found = g.V(root.id()).out("High").limit(1).toList();

    assertThat(found).hasSize(1);
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void limitOverIncidentEdgesStopsAtTheFirstOnes() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Edge> found = g.V(root.id()).outE("High").limit(1).toList();

    assertThat(found).hasSize(1);
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void existenceTestOverNeighborsStopsAtTheFirstMatch() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Object> rows = g.V(root.id()).as("r").filter(__.select("r").outE("High").inV().hasLabel("Target").as("t")).<Object>values("uid")
        .toList();

    assertThat(rows).containsExactly(1L);
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void whereOverNeighborsStopsAtTheFirstMatch() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Vertex> rows = g.V(root.id()).where(__.out("High")).toList();

    assertThat(rows).hasSize(1);
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void theIteratorReadsNothingUntilItIsPulled() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();
    final long before = lookups();

    final Iterator<Vertex> neighbors = vertex.vertices(Direction.OUT, "High");
    assertThat(neighbors.hasNext()).isTrue();
    neighbors.next();

    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void theIncidentEdgesAreNotReadUntilTheyArePulled() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();
    final long before = lookups();

    final Iterator<Edge> edges = vertex.edges(Direction.OUT, "High");
    assertThat(edges.hasNext()).isTrue();
    edges.next();

    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void orderAndDuplicatesAreThoseOfTheAdjacencyList() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();

    final List<RID> expected = new ArrayList<>();
    for (final com.arcadedb.graph.Vertex neighbor : vertex.getBaseElement().getVertices(com.arcadedb.graph.Vertex.DIRECTION.OUT, "High"))
      expected.add(neighbor.getIdentity());

    final List<RID> actual = new ArrayList<>();
    vertex.vertices(Direction.OUT, "High").forEachRemaining(v -> actual.add(((ArcadeVertex) v).getIdentity()));

    assertThat(actual).hasSize(TARGETS + 1).containsExactlyElementsOf(expected);
    assertThat(actual.stream().distinct().count()).isEqualTo(TARGETS);

    final List<RID> expectedEdges = new ArrayList<>();
    for (final com.arcadedb.graph.Edge edge : vertex.getBaseElement().getEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "High"))
      expectedEdges.add(edge.getIdentity());
    final List<RID> actualEdges = new ArrayList<>();
    vertex.edges(Direction.OUT, "High").forEachRemaining(e -> actualEdges.add(((ArcadeEdge) e).getIdentity()));
    assertThat(actualEdges).containsExactlyElementsOf(expectedEdges);
  }

  @Test
  void aNeighborDeletedAfterTheIteratorWasCreatedIsNotReturned() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();
    final GraphTraversalSource g = graph.traversal();

    final List<Object> expected = new ArrayList<>();
    g.V(root.id()).out("High").forEachRemaining(v -> expected.add(v.id()));

    final Iterator<Vertex> neighbors = vertex.vertices(Direction.OUT, "High");
    assertThat(neighbors.next().id()).isEqualTo(expected.get(0));
    expected.remove(0);

    // THE LAST NEIGHBOR LISTED, WHICH THE WALK HAS NOT REACHED YET
    final Object last = expected.get(expected.size() - 1);
    g.V(last).drop().iterate();
    expected.removeIf(last::equals);

    final List<Object> rest = new ArrayList<>();
    neighbors.forEachRemaining(v -> rest.add(v.id()));

    assertThat(rest).containsExactlyElementsOf(expected);
  }

  @Test
  void anElementRemovedAfterItWasPulledAheadIsNotHandedOut() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();
    final GraphTraversalSource g = graph.traversal();
    final List<Object> expected = new ArrayList<>();
    g.V(root.id()).out("High").forEachRemaining(v -> expected.add(v.id()));

    final Iterator<Vertex> neighbors = vertex.vertices(Direction.OUT, "High");
    // PULLS THE FIRST NEIGHBOR AHEAD, WHICH IS THEN REMOVED BEFORE ANYONE ASKS FOR IT
    assertThat(neighbors.hasNext()).isTrue();
    g.V(expected.get(0)).drop().iterate();

    assertThat(neighbors.next().id()).isEqualTo(expected.get(1));
  }

  @Test
  void aVertexRemovedAfterItsFirstNeighborWasPulledAheadHasNoMore() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();
    final GraphTraversalSource g = graph.traversal();

    final Iterator<Vertex> neighbors = vertex.vertices(Direction.OUT, "High");
    assertThat(neighbors.hasNext()).isTrue();
    g.V(root.id()).drop().iterate();

    assertThat(neighbors.hasNext()).isFalse();
  }

  @Test
  void dropWhileWalkingTheNeighborsDeletesEveryOne() {
    final GraphTraversalSource g = graph.traversal();

    g.V(root.id()).out("High").drop().iterate();
    graph.tx().commit();

    assertThat(graph.traversal().V().hasLabel("Target").count().next()).isEqualTo(3L);
    assertThat(graph.traversal().E().hasLabel("High").count().next()).isZero();
    assertThat(graph.traversal().E().hasLabel("Ref").count().next()).isEqualTo(3L);
  }

  @Test
  void dropWhileWalkingTheIncidentEdgesDeletesEveryOne() {
    final GraphTraversalSource g = graph.traversal();

    g.V(root.id()).outE("High").drop().iterate();
    graph.tx().commit();

    assertThat(graph.traversal().E().hasLabel("High").count().next()).isZero();
    assertThat(graph.traversal().V().hasLabel("Target").count().next()).isEqualTo((long) TARGETS + 3);
  }

  @Test
  void dropWhileWalkingAllTheEdgesOfTheVertexDeletesEveryOne() {
    final GraphTraversalSource g = graph.traversal();

    g.V(root.id()).bothE().drop().iterate();
    graph.tx().commit();

    assertThat(graph.traversal().E().count().next()).isZero();
  }

  @Test
  void dropWhileWalkingEdgesThatAreStillUncommittedDeletesEveryOne() {
    // THE SEGMENTS OF THE LIST ARE THE TRANSACTION'S OWN PAGES, WRITTEN TO WHILE THEY ARE WALKED
    final Vertex fresh = graph.addVertex(T.label, "Root", "uid", 2L);
    for (int i = 0; i < TARGETS; i++)
      fresh.addEdge("High", graph.addVertex(T.label, "Target", "uid", 2_000_000L + i));
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.V(fresh.id()).out("High").count().next()).isEqualTo((long) TARGETS);

    g.V(fresh.id()).out("High").drop().iterate();

    assertThat(g.V(fresh.id()).outE("High").count().next()).isZero();
    assertThat(g.V().hasLabel("Target").count().next()).isEqualTo((long) TARGETS + 3);

    graph.tx().commit();
    assertThat(graph.traversal().V().hasLabel("Target").count().next()).isEqualTo((long) TARGETS + 3);
  }

  @Test
  void aVertexRemovedWhileItsNeighborsAreWalkedEndsTheWalk() {
    final GraphTraversalSource g = graph.traversal();

    // THE ROOT GOES AT THE FIRST NEIGHBOR: ITS DELETE FREES THE LISTS THE WALK WOULD GO ON TO READ
    final long seen = g.V(root.id()).as("a").out("High").sideEffect(__.select("a").drop()).count().next();

    assertThat(seen).isBetween(1L, (long) TARGETS + 1);
    graph.tx().commit();
    assertThat(graph.traversal().V().hasLabel("Root").count().next()).isZero();
    assertThat(graph.traversal().E().count().next()).isZero();
  }

  @Test
  void aVertexDeletedByAStatementWhileItsNeighborsAreWalkedEndsTheWalk() {
    final ArcadeVertex vertex = (ArcadeVertex) graph.vertices(root.id()).next();
    final Iterator<Vertex> neighbors = vertex.vertices(Direction.OUT, "High");
    for (int i = 0; i < 10; i++)
      neighbors.next();

    // NOT THROUGH THE GRAPH: IT NEVER LEARNS THE VERTEX IS GONE, AND THE LISTS ITS DELETE FREES ARE THE ONES THE WALK GOES ON TO READ
    graph.getDatabase().command("sql", "DELETE FROM Root").close();

    int rest = 0;
    while (neighbors.hasNext()) {
      neighbors.next();
      ++rest;
    }
    // THE WALK ENDS WHERE THE LISTS THE DELETE FREED WOULD HAVE TO BE READ: IT DOES NOT GO ON THROUGH THE WHOLE REMAINDER
    assertThat(rest).isLessThan(TARGETS - 10 + 1);
  }

  @Test
  void aTraversalStillReturnsEveryNeighbor() {
    final GraphTraversalSource g = graph.traversal();

    assertThat(g.V(root.id()).out("High").toList()).hasSize(TARGETS + 1);
    assertThat(g.V(root.id()).out("High").dedup().count().next()).isEqualTo((long) TARGETS);
    assertThat(g.V(root.id()).out("Ref").count().next()).isEqualTo(3L);
    assertThat(g.V(root.id()).out().count().next()).isEqualTo((long) TARGETS + 4);
    assertThat(g.V(root.id()).outE("High").inV().count().next()).isEqualTo((long) TARGETS + 1);
  }
}
