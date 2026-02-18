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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Cypher reduce() function and shortestPath()/allShortestPaths() patterns.
 * These features are required for LDBC SNB Interactive benchmark support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherReduceAndShortestPathTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-reduce-shortestpath").create();

    // Create schema for graph tests
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("FRIEND_OF");

    // Create test data: a small social network
    // Alice --KNOWS--> Bob --KNOWS--> Charlie --KNOWS--> David
    //   |                  \--KNOWS--> Eve
    //   \--KNOWS--> Frank
    database.transaction(() -> {
      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").set("id", 1L).save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").set("id", 2L).save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").set("id", 3L).save();
      final MutableVertex david = database.newVertex("Person").set("name", "David").set("id", 4L).save();
      final MutableVertex eve = database.newVertex("Person").set("name", "Eve").set("id", 5L).save();
      final MutableVertex frank = database.newVertex("Person").set("name", "Frank").set("id", 6L).save();

      // Create edges (bidirectional)
      alice.newEdge("KNOWS", bob, true, (Object[]) null).save();
      bob.newEdge("KNOWS", charlie, true, (Object[]) null).save();
      charlie.newEdge("KNOWS", david, true, (Object[]) null).save();
      bob.newEdge("KNOWS", eve, true, (Object[]) null).save();
      alice.newEdge("KNOWS", frank, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ============================================================================
  // reduce() Function Tests
  // ============================================================================

  @Test
  void reduceSum() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 0, n IN [1, 2, 3, 4, 5] | total + n) AS sum");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Long) result.getProperty("sum")).isEqualTo(Long.valueOf(15L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void reduceProduct() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(product = 1, n IN [1, 2, 3, 4] | product * n) AS factorial");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat((Long) result.getProperty("factorial")).isEqualTo(Long.valueOf(24L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void reduceStringConcat() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(s = '', x IN ['a', 'b', 'c'] | s + x) AS concat");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<String>getProperty("concat")).isEqualTo("abc");
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void reduceWithEmptyList() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 100, n IN [] | total + n) AS sum");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Empty list should return initial value
    assertThat((Long) result.getProperty("sum")).isEqualTo(Long.valueOf(100L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void reduceWithSingleElement() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 10, n IN [5] | total + n) AS sum");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("sum")).isEqualTo(Long.valueOf(15L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void reduceWithRange() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 0, n IN range(1, 10) | total + n) AS sum");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Sum of 1..10 = 55
    assertThat(result.<Long>getProperty("sum")).isEqualTo(Long.valueOf(55L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void reduceWithMatchResults() {
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (p:Person)
            WITH collect(p.id) AS ids
            RETURN reduce(total = 0, id IN ids | total + id) AS sumIds""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Sum of person IDs: 1 + 2 + 3 + 4 + 5 + 6 = 21
    assertThat(result.<Long>getProperty("sumIds")).isEqualTo(Long.valueOf(21L));
    assertThat(resultSet.hasNext()).isFalse();
  }

  // ============================================================================
  // shortestPath() Pattern Tests
  // ============================================================================

  @Test
  void shortestPathSimple() {
    // First verify the data is there
    ResultSet countResult = database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS cnt");
    assertThat(countResult.hasNext()).isTrue();
    long cnt = countResult.next().getProperty("cnt");
    assertThat(cnt).isEqualTo(6); // We have 6 persons

    // Also verify edges exist
    ResultSet edgeResult = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name AS name");
    assertThat(edgeResult.hasNext()).isTrue();

    // Find shortest path from Alice to David (Alice->Bob->Charlie->David)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(d:Person {name: 'David'})) RETURN p");

    assertThat(resultSet.hasNext()).as("Expected shortest path result but got empty result set").isTrue();
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertThat(path).isNotNull();
    assertThat(path).isInstanceOf(List.class);

    final List<?> pathList = (List<?>) path;
    // Path should be: Alice, edge, Bob, edge, Charlie, edge, David (4 nodes + 3 edges = 7)
    assertThat(pathList.size()).isEqualTo(7);
    assertThat(pathList.get(0)).isInstanceOf(Vertex.class);
    assertThat(pathList.get(1)).isInstanceOf(Edge.class);
    assertThat(pathList.get(2)).isInstanceOf(Vertex.class);

    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shortestPathDirect() {
    // Find shortest path from Alice to Bob (direct connection)
    final ResultSet resultSet = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
        p = shortestPath((a)-[:KNOWS*]-(b)) \
        RETURN p""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertThat(path).isNotNull();
    assertThat(path).isInstanceOf(List.class);

    final List<?> pathList = (List<?>) path;
    // Path should be: Alice, edge, Bob (2 nodes + 1 edge = 3)
    assertThat(pathList.size()).isEqualTo(3);

    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shortestPathSameNode() {
    // Path from a node to itself
    final ResultSet resultSet = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Alice'}), \
        p = shortestPath((a)-[:KNOWS*]-(b)) \
        RETURN p""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertThat(path).isNotNull();
    assertThat(path).isInstanceOf(List.class);

    final List<?> pathList = (List<?>) path;
    // Path to self should be just the node itself (resolved as Vertex)
    assertThat(pathList.size()).isEqualTo(1);
    assertThat(pathList.get(0)).isInstanceOf(Vertex.class);

    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shortestPathNoConnection() {
    // Create an isolated node
    database.transaction(() -> database.newVertex("Person").set("name", "Isolated").set("id", 99L).save());

    // Try to find path to isolated node (should return no results)
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Isolated'}),
            p = shortestPath((a)-[:KNOWS*]-(b))
            RETURN p""");

    // No path should be found - either empty result set or null path
    if (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final Object path = result.getProperty("p");
      assertThat(path == null || (path instanceof List && ((List<?>) path).isEmpty())).isTrue();
    }
  }

  @Test
  void shortestPathWithDirectedEdges() {
    // Test with directed traversal (OUT direction)
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'David'}),
            p = shortestPath((a)-[:KNOWS*]->(d))
            RETURN p""");

    // Should find path following edge direction
    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object path = result.getProperty("p");
    assertThat(path).isNotNull();
  }

  @Test
  void shortestPathBetweenTwoHops() {
    // Find shortest path from Alice to Eve (Alice->Bob->Eve)
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (e:Person {name: 'Eve'}),
            p = shortestPath((a)-[:KNOWS*]-(e))
            RETURN p""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertThat(path).isNotNull();
    assertThat(path).isInstanceOf(List.class);

    final List<?> pathList = (List<?>) path;
    // Path should be: Alice, edge, Bob, edge, Eve (3 nodes + 2 edges = 5)
    assertThat(pathList.size()).isEqualTo(5);

    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void allShortestPaths() {
    // Test allShortestPaths (currently returns single path, but tests parsing)
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}),
            p = allShortestPaths((a)-[:KNOWS*]-(b))
            RETURN p""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    final Object path = result.getProperty("p");
    assertThat(path).isNotNull();
  }

  @Test
  void shortestPathWithPathVariable() {
    // Test that path variable is properly bound and length() works correctly
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'David'}),
            path = shortestPath((a)-[:KNOWS*]-(d))
            RETURN path, length(path) AS pathLength""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();

    final Object path = result.getProperty("path");
    assertThat(path).isNotNull();
    // Alice -> Bob -> Charlie -> David = 3 relationships
    assertThat(result.<Long>getProperty("pathLength")).isEqualTo(3L);
  }

  @Test
  void shortestPathLengthReturnsCorrectHops() {
    // Regression test for https://github.com/ArcadeData/arcadedb/issues/3332
    // length(path) on shortestPath was returning 0 instead of the actual number of hops
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'David'}),
            path = shortestPath((a)-[:KNOWS*]-(d))
            RETURN length(path) AS hops""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Alice -> Bob -> Charlie -> David = 3 hops
    assertThat(result.<Long>getProperty("hops")).isEqualTo(3L);
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shortestPathLengthDirectConnection() {
    // Direct connection: length should be 1
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}),
            path = shortestPath((a)-[:KNOWS*]-(b))
            RETURN length(path) AS hops""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    assertThat(result.<Long>getProperty("hops")).isEqualTo(1L);
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void shortestPathNodesAndRelationships() {
    // Test that nodes() and relationships() work on shortest paths
    final ResultSet resultSet = database.query("opencypher",
        """
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}),
            path = shortestPath((a)-[:KNOWS*]-(b))
            RETURN nodes(path) AS pathNodes, relationships(path) AS pathRels""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();

    final List<?> pathNodes = result.getProperty("pathNodes");
    assertThat(pathNodes).hasSize(2); // Alice, Bob

    final List<?> pathRels = result.getProperty("pathRels");
    assertThat(pathRels).hasSize(1); // one KNOWS edge

    assertThat(resultSet.hasNext()).isFalse();
  }

  // ============================================================================
  // Integration Tests (reduce + shortestPath combined)
  // ============================================================================

  @Test
  void reduceInListComprehension() {
    // Use reduce with collected values
    final ResultSet resultSet = database.query("opencypher",
        """
            WITH [1, 2, 3, 4, 5] AS numbers
            RETURN reduce(sum = 0, x IN [n IN numbers WHERE n > 2] | sum + x) AS filteredSum""");

    assertThat(resultSet.hasNext()).isTrue();
    final Result result = resultSet.next();
    // Sum of 3 + 4 + 5 = 12
    assertThat((Long) result.getProperty("filteredSum")).isEqualTo(Long.valueOf(12L));
    assertThat(resultSet.hasNext()).isFalse();
  }
}
