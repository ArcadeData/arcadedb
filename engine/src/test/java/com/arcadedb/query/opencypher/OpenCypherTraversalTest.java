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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.traversal.BreadthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.DepthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for graph traversal implementations.
 * Phase 2: Tests for BFS, DFS, and variable-length path traversal.
 */
public class OpenCypherTraversalTest {
  private Database database;
  private Vertex alice;
  private Vertex bob;
  private Vertex charlie;
  private Vertex david;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-traversal").create();
    setupTestGraph();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void setupTestGraph() {
    // Create a simple chain: Alice -> Bob -> Charlie -> David
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      alice = database.newVertex("Person").set("name", "Alice").save();
      bob = database.newVertex("Person").set("name", "Bob").save();
      charlie = database.newVertex("Person").set("name", "Charlie").save();
      david = database.newVertex("Person").set("name", "David").save();

      ((MutableVertex) alice).newEdge("KNOWS", bob, true, (Object[]) null).save();
      ((MutableVertex) bob).newEdge("KNOWS", charlie, true, (Object[]) null).save();
      ((MutableVertex) charlie).newEdge("KNOWS", david, true, (Object[]) null).save();
    });
  }

  @Test
  void testBreadthFirstTraversal() {
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 1, 3, false, true);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find: Bob (1 hop), Charlie (2 hops), David (3 hops)
    assertThat(results).hasSize(3);

    // BFS should return vertices in order of distance
    assertThat(results.get(0).get("name")).isEqualTo("Bob");
    assertThat(results.get(1).get("name")).isEqualTo("Charlie");
    assertThat(results.get(2).get("name")).isEqualTo("David");
  }

  @Test
  void testBreadthFirstTraversalWithMinHops() {
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 2, 3, false, true);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find: Charlie (2 hops), David (3 hops) - Bob is skipped (only 1 hop)
    assertThat(results).hasSize(2);
    assertThat(results.get(0).get("name")).isEqualTo("Charlie");
    assertThat(results.get(1).get("name")).isEqualTo("David");
  }

  @Test
  void testBreadthFirstTraversalLimited() {
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 1, 2, false, true);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find: Bob (1 hop), Charlie (2 hops) - David is beyond max hops
    assertThat(results).hasSize(2);
    assertThat(results.get(0).get("name")).isEqualTo("Bob");
    assertThat(results.get(1).get("name")).isEqualTo("Charlie");
  }

  @Test
  void testDepthFirstTraversal() {
    final DepthFirstTraverser traverser = new DepthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 1, 3, false, true);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find: Bob, Charlie, David
    assertThat(results).hasSize(3);

    // All three vertices should be found
    final List<String> names = results.stream().map(v -> (String) v.get("name")).toList();
    assertThat(names).containsExactlyInAnyOrder("Bob", "Charlie", "David");
  }

  @Test
  void testBreadthFirstPathTraversal() {
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 1, 3, true, true);

    final Iterator<TraversalPath> paths = traverser.traversePaths(alice);
    final List<TraversalPath> results = new ArrayList<>();
    paths.forEachRemaining(results::add);

    assertThat(results).hasSize(3);

    // Check first path: Alice -> Bob
    final TraversalPath path1 = results.get(0);
    assertThat(path1.getVertices()).hasSize(2);
    assertThat(path1.getEdges()).hasSize(1);
    assertThat(path1.length()).isEqualTo(1);
    assertThat(path1.getStartVertex().get("name")).isEqualTo("Alice");
    assertThat(path1.getEndVertex().get("name")).isEqualTo("Bob");

    // Check second path: Alice -> Bob -> Charlie
    final TraversalPath path2 = results.get(1);
    assertThat(path2.getVertices()).hasSize(3);
    assertThat(path2.getEdges()).hasSize(2);
    assertThat(path2.length()).isEqualTo(2);
    assertThat(path2.getEndVertex().get("name")).isEqualTo("Charlie");

    // Check third path: Alice -> Bob -> Charlie -> David
    final TraversalPath path3 = results.get(2);
    assertThat(path3.getVertices()).hasSize(4);
    assertThat(path3.getEdges()).hasSize(3);
    assertThat(path3.length()).isEqualTo(3);
    assertThat(path3.getEndVertex().get("name")).isEqualTo("David");
  }

  @Test
  void testVariableLengthPathTraverser() {
    final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(Direction.OUT, new String[]{"KNOWS"}, 1, 2,
        false);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find: Bob (1 hop), Charlie (2 hops)
    assertThat(results).hasSize(2);
  }

  @Test
  void testVariableLengthPathTraverserWithPaths() {
    final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(Direction.OUT, new String[]{"KNOWS"}, 2, 3,
        true);

    final Iterator<TraversalPath> paths = traverser.traversePaths(alice);
    final List<TraversalPath> results = new ArrayList<>();
    paths.forEachRemaining(results::add);

    // Should find paths with 2 and 3 hops
    assertThat(results).hasSize(2);

    // First path should be 2 hops: Alice -> Bob -> Charlie
    assertThat(results.get(0).length()).isEqualTo(2);
    assertThat(results.get(0).getEndVertex().get("name")).isEqualTo("Charlie");

    // Second path should be 3 hops: Alice -> Bob -> Charlie -> David
    assertThat(results.get(1).length()).isEqualTo(3);
    assertThat(results.get(1).getEndVertex().get("name")).isEqualTo("David");
  }

  @Test
  void testTraversalWithTypeFilter() {
    // Add a different edge type
    database.getSchema().createEdgeType("LIKES");

    database.transaction(() -> {
      ((MutableVertex) alice).newEdge("LIKES", charlie, true, (Object[]) null).save();
    });

    // Traverse only KNOWS relationships
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 1, 3, false, true);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find: Bob, Charlie, David via KNOWS only
    assertThat(results).hasSize(3);
  }

  @Test
  void testTraversalWithoutTypeFilter() {
    // Add a different edge type
    database.getSchema().createEdgeType("LIKES");

    database.transaction(() -> {
      ((MutableVertex) alice).newEdge("LIKES", charlie, true, (Object[]) null).save();
    });

    // Traverse all relationship types
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, null, 1, 2, false, true);

    final Iterator<Vertex> vertices = traverser.traverse(alice);
    final List<Vertex> results = new ArrayList<>();
    vertices.forEachRemaining(results::add);

    // Should find at least Bob and Charlie (Charlie via both KNOWS and LIKES)
    assertThat(results.size()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void testTraversalPathContainsVertex() {
    final BreadthFirstTraverser traverser = new BreadthFirstTraverser(Direction.OUT, new String[]{"KNOWS"}, 3, 3, true, true);

    final Iterator<TraversalPath> paths = traverser.traversePaths(alice);

    if (paths.hasNext()) {
      final TraversalPath path = paths.next();

      assertThat(path.containsVertex(alice)).isTrue();
      assertThat(path.containsVertex(bob)).isTrue();
      assertThat(path.containsVertex(charlie)).isTrue();
      assertThat(path.containsVertex(david)).isTrue();
    }
  }
}
