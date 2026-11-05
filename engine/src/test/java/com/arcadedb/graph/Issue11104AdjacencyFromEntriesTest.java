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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11104: the adjacency the {@code algo.*} procedures fall back to without a Graph Analytical View
 * ({@link GraphEngine#buildAdjacencyList}) loaded every edge record and resolved its far end through a boxed
 * {@code Map<RID, Integer>}, twice per edge. It now reads the edge-list entries in place ({@link EdgeListScanner}) and resolves
 * the far end through a primitive vertex index.
 * <p>
 * The contract is the answer of the record walk, row by row and in the same order, which {@link #recordWalk} restates as it was
 * written; the one deliberate difference is a ghost entry, which is followed as the view build follows it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11104AdjacencyFromEntriesTest {
  private static final String DB_PATH = "./target/databases/issue-11104";

  private static final Vertex.DIRECTION[] DIRECTIONS = { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN, Vertex.DIRECTION.BOTH };

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("AdjNode");
    database.getSchema().createVertexType("AdjOther");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().buildEdgeType().withName("LIKES").create();
    database.getSchema().buildEdgeType().withName("LOVES").withSuperType("LIKES").create();
    database.getSchema().buildEdgeType().withName("LIGHT").withLightweight(true).create();
    database.getSchema().buildEdgeType().withName("ONEWAY").withBidirectional(false).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void theRowsAreTheRecordWalksRowsInTheSameOrder() {
    final List<Vertex> vertices = randomGraph(120, 900, new String[] { "KNOWS", "LIKES", "LOVES", "LIGHT" });
    final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);

    for (final String[] types : new String[][] { null, {}, { "KNOWS" }, { "LIKES" }, { "LOVES" }, { "LIGHT" }, { "KNOWS", "LOVES" },
        { "NoSuchType" } })
      for (final Vertex.DIRECTION dir : DIRECTIONS)
        assertThat(GraphEngine.buildAdjacencyList(vertices, index, dir, types))
            .as("%s %s", dir, Arrays.toString(types))
            .isDeepEqualTo(recordWalk(vertices, index, dir, types));
  }

  @Test
  void aSubsetOfTheVerticesOnlyListsNeighboursOfTheSubset() {
    final List<Vertex> all = randomGraph(80, 500, new String[] { "KNOWS", "LIKES" });
    // Every other vertex, in reverse: the rows follow the list, the neighbours the map, and a vertex left out is no neighbour
    final List<Vertex> subset = new ArrayList<>();
    for (int i = all.size() - 1; i >= 0; i -= 2)
      subset.add(all.get(i));
    final Map<RID, Integer> index = GraphEngine.buildRidIndex(subset);

    for (final Vertex.DIRECTION dir : DIRECTIONS)
      assertThat(GraphEngine.buildAdjacencyList(subset, index, dir, null)).as(dir.name())
          .isDeepEqualTo(recordWalk(subset, index, dir, null));
  }

  @Test
  void aUnidirectionalTypeIsTransposedIntoTheIncomingRows() {
    final List<Vertex> vertices = randomGraph(60, 400, new String[] { "ONEWAY", "KNOWS" });
    final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);

    for (final String[] types : new String[][] { null, { "ONEWAY" }, { "KNOWS" }, { "ONEWAY", "KNOWS" } })
      for (final Vertex.DIRECTION dir : DIRECTIONS)
        assertThat(GraphEngine.buildAdjacencyList(vertices, index, dir, types)).as("%s %s", dir, Arrays.toString(types))
            .isDeepEqualTo(transposedRecordWalk(vertices, index, dir, types));
  }

  @Test
  void theEntryCountIsHandedOverBeforeTheRowsAreAllocated() {
    final List<Vertex> vertices = randomGraph(40, 300, new String[] { "KNOWS" });
    final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);
    final AtomicLong counted = new AtomicLong(-1);

    final int[][] adj = GraphEngine.buildAdjacencyList(vertices, index, Vertex.DIRECTION.BOTH, null, counted::set);
    assertThat(counted.get()).isEqualTo(Arrays.stream(adj).mapToLong(row -> row.length).sum()).isPositive();

    counted.set(-1);
    assertThat(GraphEngine.buildAdjacencyList(List.of(), Map.of(), Vertex.DIRECTION.OUT, null, counted::set)).isEmpty();
    assertThat(counted.get()).isZero();
  }

  @Test
  void theEdgesOfTheTransactionInProgressAreListed() {
    final List<Vertex> vertices = randomGraph(30, 100, new String[] { "KNOWS" });
    database.begin();
    try {
      final Vertex a = vertices.get(3).asVertex(true);
      final Vertex b = vertices.get(7).asVertex(true);
      a.modify().newEdge("KNOWS", b).save();
      final List<Vertex> reloaded = reload(vertices);
      final Map<RID, Integer> index = GraphEngine.buildRidIndex(reloaded);
      for (final Vertex.DIRECTION dir : DIRECTIONS)
        assertThat(GraphEngine.buildAdjacencyList(reloaded, index, dir, null)).as(dir.name())
            .isDeepEqualTo(recordWalk(reloaded, index, dir, null));
    } finally {
      database.rollback();
    }
  }

  @Test
  void aGhostEntryIsFollowedAsTheViewBuildFollowsIt() {
    final MutableVertex[] v = new MutableVertex[3];
    final Edge[] edges = new Edge[2];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        v[i] = database.newVertex("AdjNode").set("i", i).save();
      edges[0] = v[0].newEdge("KNOWS", v[1]).save();
      edges[1] = v[1].newEdge("KNOWS", v[2]).save();
    });
    // The edge record goes, the entry stays: a ghost (see GhostEdgeReporter)
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, edges[0].getIdentity()));
    final List<Vertex> vertices = reload(List.of(v[0], v[1], v[2]));
    final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);

    // No edge record is read, so the ghost cannot be told from a live entry: it is followed, as the view build follows it
    assertThat(GraphEngine.buildAdjacencyList(vertices, index, Vertex.DIRECTION.OUT, null)).isDeepEqualTo(new int[][] { { 1 }, { 2 }, {} });
    assertThat(GraphEngine.buildAdjacencyList(vertices, index, Vertex.DIRECTION.IN, null)).isDeepEqualTo(new int[][] { {}, { 0 }, { 1 } });

    // CHECK DATABASE FIX prunes it, after which every walk agrees
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      rs.hasNext();
    }
    final List<Vertex> repaired = reload(vertices);
    assertThat(GraphEngine.buildAdjacencyList(repaired, index, Vertex.DIRECTION.OUT, null)).isDeepEqualTo(new int[][] { {}, { 2 }, {} });
    assertThat(GraphEngine.buildAdjacencyList(repaired, index, Vertex.DIRECTION.OUT, null))
        .isDeepEqualTo(recordWalk(repaired, index, Vertex.DIRECTION.OUT, null));
  }

  // ---------------------------------------------------------------------------------------------------------------------------

  /** Vertices of two types, edges of the given types between random pairs, self-loops and parallel edges included. */
  private List<Vertex> randomGraph(final int vertexCount, final int edgeCount, final String[] edgeTypes) {
    final Random random = new Random(11104);
    final List<Vertex> vertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < vertexCount; i++)
        vertices.add(database.newVertex(i % 3 == 0 ? "AdjOther" : "AdjNode").set("i", i).save());
      for (int e = 0; e < edgeCount; e++) {
        final MutableVertex from = vertices.get(random.nextInt(vertexCount)).modify();
        final Vertex to = vertices.get(random.nextInt(vertexCount));
        from.newEdge(edgeTypes[random.nextInt(edgeTypes.length)], to).save();
      }
    });
    return reload(vertices);
  }

  private List<Vertex> reload(final List<? extends Vertex> vertices) {
    final List<Vertex> reloaded = new ArrayList<>(vertices.size());
    for (final Vertex vertex : vertices)
      reloaded.add(vertex.getIdentity().asVertex(true));
    return reloaded;
  }

  /** The record walk {@code GraphEngine.buildAdjacencyList} was before #11104, for the bidirectional edge types. */
  private static int[][] recordWalk(final List<Vertex> vertices, final Map<RID, Integer> index, final Vertex.DIRECTION dir,
      final String[] types) {
    final int[][] adj = new int[vertices.size()][];
    for (int i = 0; i < vertices.size(); i++) {
      final Vertex v = vertices.get(i);
      final List<Integer> row = new ArrayList<>();
      for (final Edge e : types != null && types.length > 0 ? v.getEdges(dir, types) : v.getEdges(dir)) {
        try {
          final Integer j = index.get(GraphEngine.neighborRid(e, v.getIdentity(), dir));
          if (j != null)
            row.add(j);
        } catch (final RecordNotFoundException ghost) {
          // skipped by the record walk
        }
      }
      adj[i] = row.stream().mapToInt(Integer::intValue).toArray();
    }
    return adj;
  }

  /** The record walk over a set of types that includes a unidirectional one, as it was before #11104 (issue #8629). */
  private int[][] transposedRecordWalk(final List<Vertex> vertices, final Map<RID, Integer> index, final Vertex.DIRECTION dir,
      final String[] types) {
    final boolean typed = types != null && types.length > 0;
    final String[] unidirectional = dir == Vertex.DIRECTION.OUT ? new String[0] :
        IncomingEdgeLookup.getUnidirectionalTypes(database.getSchema(), types);
    if (unidirectional.length == 0)
      return recordWalk(vertices, index, dir, types);
    final List<String> unidirectionalTypes = List.of(unidirectional);

    final List<List<Integer>> rows = new ArrayList<>();
    for (int i = 0; i < vertices.size(); i++)
      rows.add(new ArrayList<>());
    final boolean both = dir == Vertex.DIRECTION.BOTH;
    for (int i = 0; i < vertices.size(); i++) {
      final Vertex v = vertices.get(i);
      for (final Edge e : both ? (typed ? v.getEdges(Vertex.DIRECTION.OUT, types) : v.getEdges(Vertex.DIRECTION.OUT)) :
          v.getEdges(Vertex.DIRECTION.OUT, unidirectional)) {
        final Integer j = index.get(e.getIn());
        if (j == null)
          continue;
        if (both)
          rows.get(i).add(j);
        if (unidirectionalTypes.contains(e.getTypeName()))
          rows.get(j).add(i);
      }
      for (final Edge e : typed ? v.getEdges(Vertex.DIRECTION.IN, types) : v.getEdges(Vertex.DIRECTION.IN)) {
        if (unidirectionalTypes.contains(e.getTypeName()))
          continue;
        final Integer j = index.get(e.getOut());
        if (j != null)
          rows.get(i).add(j);
      }
    }
    final int[][] adj = new int[rows.size()][];
    for (int i = 0; i < rows.size(); i++)
      adj[i] = rows.get(i).stream().mapToInt(Integer::intValue).toArray();
    return adj;
  }
}
