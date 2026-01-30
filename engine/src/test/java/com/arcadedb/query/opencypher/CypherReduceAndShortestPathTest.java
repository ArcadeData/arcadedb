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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Cypher reduce() function and shortestPath()/allShortestPaths() patterns.
 * These features are required for LDBC SNB Interactive benchmark support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherReduceAndShortestPathTest {
  private Database database;

  @BeforeEach
  public void setup() {
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
  public void teardown() {
    if (database != null)
      database.drop();
  }

  // ============================================================================
  // reduce() Function Tests
  // ============================================================================

  @Test
  public void testReduceSum() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 0, n IN [1, 2, 3, 4, 5] | total + n) AS sum");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(15L), (Long) result.getProperty("sum"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testReduceProduct() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(product = 1, n IN [1, 2, 3, 4] | product * n) AS factorial");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(24L), (Long) result.getProperty("factorial"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testReduceStringConcat() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(s = '', x IN ['a', 'b', 'c'] | s + x) AS concat");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals("abc", result.getProperty("concat"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testReduceWithEmptyList() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 100, n IN [] | total + n) AS sum");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    // Empty list should return initial value
    assertEquals(Long.valueOf(100L), (Long) result.getProperty("sum"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testReduceWithSingleElement() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 10, n IN [5] | total + n) AS sum");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    assertEquals(Long.valueOf(15L), (Long) result.getProperty("sum"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testReduceWithRange() {
    final ResultSet resultSet = database.query("opencypher",
        "RETURN reduce(total = 0, n IN range(1, 10) | total + n) AS sum");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    // Sum of 1..10 = 55
    assertEquals(Long.valueOf(55L), (Long) result.getProperty("sum"));
    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testReduceWithMatchResults() {
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (p:Person) " +
        "WITH collect(p.id) AS ids " +
        "RETURN reduce(total = 0, id IN ids | total + id) AS sumIds");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    // Sum of person IDs: 1 + 2 + 3 + 4 + 5 + 6 = 21
    assertEquals(Long.valueOf(21L), (Long) result.getProperty("sumIds"));
    assertFalse(resultSet.hasNext());
  }

  // ============================================================================
  // shortestPath() Pattern Tests
  // ============================================================================

  @Test
  public void testShortestPathSimple() {
    // First verify the data is there
    ResultSet countResult = database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS cnt");
    assertTrue(countResult.hasNext());
    long cnt = (Long) countResult.next().getProperty("cnt");
    assertEquals(6, cnt); // We have 6 persons

    // Also verify edges exist
    ResultSet edgeResult = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name AS name");
    assertTrue(edgeResult.hasNext());

    // Find shortest path from Alice to David (Alice->Bob->Charlie->David)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(d:Person {name: 'David'})) " +
        "RETURN p");

    assertTrue(resultSet.hasNext(), "Expected shortest path result but got empty result set");
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertNotNull(path);
    assertTrue(path instanceof List);

    @SuppressWarnings("unchecked")
    final List<RID> pathList = (List<RID>) path;
    // Path should be: Alice, Bob, Charlie, David (4 nodes)
    assertEquals(4, pathList.size());

    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testShortestPathDirect() {
    // Find shortest path from Alice to Bob (direct connection)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), " +
        "p = shortestPath((a)-[:KNOWS*]-(b)) " +
        "RETURN p");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertNotNull(path);
    assertTrue(path instanceof List);

    @SuppressWarnings("unchecked")
    final List<RID> pathList = (List<RID>) path;
    // Path should be: Alice, Bob (2 nodes)
    assertEquals(2, pathList.size());

    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testShortestPathSameNode() {
    // Path from a node to itself
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Alice'}), " +
        "p = shortestPath((a)-[:KNOWS*]-(b)) " +
        "RETURN p");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertNotNull(path);
    assertTrue(path instanceof List);

    @SuppressWarnings("unchecked")
    final List<RID> pathList = (List<RID>) path;
    // Path to self should be just the node itself
    assertEquals(1, pathList.size());

    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testShortestPathNoConnection() {
    // Create an isolated node
    database.transaction(() -> {
      database.newVertex("Person").set("name", "Isolated").set("id", 99L).save();
    });

    // Try to find path to isolated node (should return no results)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Isolated'}), " +
        "p = shortestPath((a)-[:KNOWS*]-(b)) " +
        "RETURN p");

    // No path should be found - either empty result set or null path
    if (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final Object path = result.getProperty("p");
      assertTrue(path == null || (path instanceof List && ((List<?>) path).isEmpty()));
    }
  }

  @Test
  public void testShortestPathWithDirectedEdges() {
    // Test with directed traversal (OUT direction)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'David'}), " +
        "p = shortestPath((a)-[:KNOWS*]->(d)) " +
        "RETURN p");

    // Should find path following edge direction
    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object path = result.getProperty("p");
    assertNotNull(path);
  }

  @Test
  public void testShortestPathBetweenTwoHops() {
    // Find shortest path from Alice to Eve (Alice->Bob->Eve)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (e:Person {name: 'Eve'}), " +
        "p = shortestPath((a)-[:KNOWS*]-(e)) " +
        "RETURN p");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();

    final Object path = result.getProperty("p");
    assertNotNull(path);
    assertTrue(path instanceof List);

    @SuppressWarnings("unchecked")
    final List<RID> pathList = (List<RID>) path;
    // Path should be: Alice, Bob, Eve (3 nodes)
    assertEquals(3, pathList.size());

    assertFalse(resultSet.hasNext());
  }

  @Test
  public void testAllShortestPaths() {
    // Test allShortestPaths (currently returns single path, but tests parsing)
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), " +
        "p = allShortestPaths((a)-[:KNOWS*]-(b)) " +
        "RETURN p");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    final Object path = result.getProperty("p");
    assertNotNull(path);
  }

  @Test
  public void testShortestPathWithPathVariable() {
    // Test that path variable is properly bound
    final ResultSet resultSet = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'David'}), " +
        "path = shortestPath((a)-[:KNOWS*]-(d)) " +
        "RETURN path, length(path) AS pathLength");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();

    final Object path = result.getProperty("path");
    assertNotNull(path);
  }

  // ============================================================================
  // Integration Tests (reduce + shortestPath combined)
  // ============================================================================

  @Test
  public void testReduceInListComprehension() {
    // Use reduce with collected values
    final ResultSet resultSet = database.query("opencypher",
        "WITH [1, 2, 3, 4, 5] AS numbers " +
        "RETURN reduce(sum = 0, x IN [n IN numbers WHERE n > 2] | sum + x) AS filteredSum");

    assertTrue(resultSet.hasNext());
    final Result result = resultSet.next();
    // Sum of 3 + 4 + 5 = 12
    assertEquals(Long.valueOf(12L), (Long) result.getProperty("filteredSum"));
    assertFalse(resultSet.hasNext());
  }
}
