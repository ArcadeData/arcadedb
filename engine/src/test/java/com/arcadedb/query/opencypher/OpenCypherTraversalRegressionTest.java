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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression tests for variable-length path patterns in OpenCypher.
 * <p>
 * These tests prevent regression of critical bugs:
 * - Infinite loops due to ExpandPathStep calling prev.syncPull() repeatedly
 * - Duplicate results caused by creating new traversers for the same source vertex
 * - Visited set not working due to database-dependent RID equality
 *
 * @author Luca Garulli
 */
class OpenCypherTraversalRegressionTest extends TestHelper {

  @BeforeEach
  void setupGraph() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("Link");

      // Create a simple chain: 0 -> 1 -> 2 -> 3 -> 4
      final Vertex v0 = database.newVertex("Node").set("id", 0).save();
      final Vertex v1 = database.newVertex("Node").set("id", 1).save();
      final Vertex v2 = database.newVertex("Node").set("id", 2).save();
      final Vertex v3 = database.newVertex("Node").set("id", 3).save();
      final Vertex v4 = database.newVertex("Node").set("id", 4).save();

      v0.newEdge("Link", v1, true, (Object[]) null);
      v1.newEdge("Link", v2, true, (Object[]) null);
      v2.newEdge("Link", v3, true, (Object[]) null);
      v3.newEdge("Link", v4, true, (Object[]) null);
    });
  }

  /**
   * REGRESSION TEST: Fixed-length paths must return exactly one result per matching path.
   * <p>
   * BUG: ExpandPathStep was calling prev.syncPull() repeatedly in a loop, getting the same
   * source vertex over and over. This caused millions of duplicate results.
   * <p>
   * FIX: Cache the ResultSet from prev.syncPull() and reuse it instead of calling repeatedly.
   */
  @Test
  void fixedLengthPathNoDuplicates() {
    database.begin();
    try {
      final String query = "MATCH (a:Node {id: 0})-[:Link*3]->(b) RETURN b";
      final ResultSet result = database.query("opencypher", query);

      final Set<RID> uniqueResults = new HashSet<>();
      int count = 0;
      while (result.hasNext()) {
        final Vertex v = (Vertex) result.next().toElement();
        uniqueResults.add(v.getIdentity());
        count++;

        // Safety check: prevent infinite loop in case of regression
        if (count > 100) {
          fail("Query returned more than 100 results - REGRESSION: infinite loop bug has returned!");
        }
      }

      // Should find exactly one vertex at depth 3: vertex with id=3
      assertThat(count).as("Expected exactly 1 result for fixed-length path *3").isEqualTo(1);
      assertThat(uniqueResults.size()).as("REGRESSION: Found duplicate results! Check ExpandPathStep.prevResults caching").isEqualTo(1);
    } finally {
      database.commit();
    }
  }

  /**
   * REGRESSION TEST: Variable-length paths must return unique results only.
   * Tests range syntax *1..3 which should return vertices at depth 1, 2, and 3.
   */
  @Test
  void variableLengthPathNoDuplicates() {
    database.begin();
    try {
      final String query = "MATCH (a:Node {id: 0})-[:Link*1..3]->(b) RETURN b ORDER BY b.id";
      final ResultSet result = database.query("opencypher", query);

      final List<Integer> ids = new ArrayList<>();
      final Set<RID> uniqueResults = new HashSet<>();
      int count = 0;

      while (result.hasNext()) {
        final Vertex v = (Vertex) result.next().toElement();
        ids.add((Integer) v.get("id"));
        uniqueResults.add(v.getIdentity());
        count++;

        if (count > 100) {
          fail("Query returned more than 100 results - REGRESSION: infinite loop bug has returned!");
        }
      }

      // Should find vertices at depth 1, 2, 3: ids [1, 2, 3]
      assertThat(count).as("Expected exactly 3 results for variable-length path *1..3").isEqualTo(3);
      assertThat(uniqueResults.size()).as("REGRESSION: Found duplicate results!").isEqualTo(3);
      assertThat(ids).as("Should find vertices with ids 1, 2, 3").isEqualTo(Arrays.asList(1, 2, 3));
    } finally {
      database.commit();
    }
  }

  /**
   * REGRESSION TEST: Traversal must handle cycles without infinite loops.
   * <p>
   * BUG: Visited set was being recreated for each traversal, allowing cycles to be traversed infinitely.
   * <p>
   * FIX: ExpandPathStep now caches and reuses the ResultSet, so each source vertex gets one traverser
   * with persistent visited tracking. Cycle detection prevents revisiting vertices.
   */
  @Test
  void variableLengthPathWithCycles() {
    database.transaction(() -> {
      // Add a cycle: 4 -> 0 (back to start)
      final Vertex v0 = (Vertex) database.query("sql", "SELECT FROM Node WHERE id = 0").next().getRecord().get();
      final Vertex v4 = (Vertex) database.query("sql", "SELECT FROM Node WHERE id = 4").next().getRecord().get();
      v4.newEdge("Link", v0, true, (Object[]) null);
    });

    database.begin();
    try {
      // With cycle detection enabled, traversal should not infinite loop
      // Query for depth 4: should find vertex 4
      final String query = "MATCH (a:Node {id: 0})-[:Link*4]->(b) RETURN b";
      final ResultSet result = database.query("opencypher", query);

      final Set<RID> uniqueResults = new HashSet<>();
      int count = 0;

      while (result.hasNext()) {
        final Vertex v = (Vertex) result.next().toElement();
        uniqueResults.add(v.getIdentity());
        count++;

        if (count > 100) {
          fail("Query returned more than 100 results - REGRESSION: cycle causes infinite loop!");
        }
      }

      // Path: 0->1->2->3->4 (depth 4)
      // Cycle 4->0 is not followed because 0 is already visited
      assertThat(count).as("Expected exactly 1 result for path of length 4").isEqualTo(1);
      assertThat(uniqueResults.size()).as("REGRESSION: Found duplicate results!").isEqualTo(1);

      final Vertex target = (Vertex) database.query("opencypher", "MATCH (a:Node {id: 0})-[:Link*4]->(b) RETURN b")
          .next().toElement();
      assertThat(target.get("id")).as("Should reach vertex 4 at depth 4").isEqualTo(4);
    } finally {
      database.commit();
    }
  }

  /**
   * REGRESSION TEST: Highly connected graph should not produce duplicates.
   * Tests graph where vertices have multiple outgoing edges.
   */
  @Test
  void variableLengthPathHighlyConnectedGraph() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createEdgeType("Follows");

      // Create a small highly connected graph
      final List<Vertex> accounts = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        accounts.add(database.newVertex("Account").set("id", i).save());
      }

      // Each vertex connects to 2 others
      final int[][] edges = {{0, 1}, {0, 2}, {1, 2}, {1, 3}, {2, 3}, {2, 4}, {3, 4}};
      for (int[] edge : edges) {
        accounts.get(edge[0]).newEdge("Follows", accounts.get(edge[1]), true, (Object[]) null);
      }
    });

    database.begin();
    try {
      final String query = "MATCH (a:Account {id: 0})-[:Follows*2]->(b) RETURN b ORDER BY b.id";
      final ResultSet result = database.query("opencypher", query);

      final Set<RID> uniqueResults = new HashSet<>();
      final List<Integer> ids = new ArrayList<>();
      int count = 0;

      while (result.hasNext()) {
        final Vertex v = (Vertex) result.next().toElement();
        uniqueResults.add(v.getIdentity());
        ids.add((Integer) v.get("id"));
        count++;

        if (count > 100) {
          fail("Query returned more than 100 results - REGRESSION: returning duplicates!");
        }
      }

      // From vertex 0, depth 2 paths: 0->1->2, 0->1->3, 0->2->3, 0->2->4
      // Should find vertices {2, 3, 4} but each should appear only once
      assertThat(count <= 10).as("Should not have excessive results from highly connected graph").isTrue();
      assertThat(uniqueResults.size()).as("REGRESSION: All results should be unique (found duplicates)").isEqualTo(count);
    } finally {
      database.commit();
    }
  }

  /**
   * REGRESSION TEST: Multiple source vertices must each be processed exactly once.
   * <p>
   * BUG: ExpandPathStep was calling prev.syncPull() in a loop, which created a new ResultSet
   * each time, causing the same source vertex to be returned infinitely.
   * <p>
   * FIX: Cache prevResults and reuse it, so each source is processed only once.
   */
  @Test
  void multipleSourceVertices() {
    database.begin();
    try {
      // Match TWO source vertices (id=0 and id=1) and traverse from each
      final String query = "MATCH (a:Node) WHERE a.id IN [0, 1] MATCH (a)-[:Link*2]->(b) RETURN a.id as source, b.id as target ORDER BY source, target";
      final ResultSet result = database.query("opencypher", query);

      final Map<Integer, List<Integer>> resultsBySource = new HashMap<>();
      int count = 0;

      while (result.hasNext()) {
        final Result r = result.next();
        final int sourceId = (Integer) r.getProperty("source");
        final int targetId = (Integer) r.getProperty("target");

        resultsBySource.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(targetId);
        count++;

        if (count > 100) {
          fail("Query returned more than 100 results - REGRESSION: processing same source repeatedly!");
        }
      }

      // Source 0 -> depth 2 -> vertex 2
      // Source 1 -> depth 2 -> vertex 3
      assertThat(count).as("Expected 2 results (one from each source)").isEqualTo(2);
      assertThat(resultsBySource.size()).as("REGRESSION: Should have results from 2 different sources").isEqualTo(2);

      assertThat(resultsBySource.containsKey(0)).as("Should have result from source 0").isTrue();
      assertThat(resultsBySource.containsKey(1)).as("Should have result from source 1").isTrue();

      assertThat(resultsBySource.get(0)).as("Source 0 should reach vertex 2 at depth 2").isEqualTo(Arrays.asList(2));
      assertThat(resultsBySource.get(1)).as("Source 1 should reach vertex 3 at depth 2").isEqualTo(Arrays.asList(3));
    } finally {
      database.commit();
    }
  }

  /**
   * REGRESSION TEST: Ensure empty results don't cause infinite loops.
   * When there are no matching paths, query should return 0 results and terminate.
   */
  @Test
  void noMatchingPathsTerminates() {
    database.begin();
    try {
      // Query for non-existent edge type
      final String query = "MATCH (a:Node {id: 0})-[:NONEXISTENT*3]->(b) RETURN b";
      final ResultSet result = database.query("opencypher", query);

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;

        if (count > 100) {
          fail("Query returned results for non-existent edge type - REGRESSION!");
        }
      }

      assertThat(count).as("Should return 0 results for non-existent edge type").isEqualTo(0);
    } finally {
      database.commit();
    }
  }
}
