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
import com.arcadedb.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for named paths with variable-length relationships.
 * Note: Variable-length path queries currently have a duplication bug (pre-existing issue).
 * These tests verify that path variables ARE being stored correctly.
 */
public class OpenCypherVariableLengthPathTest {
  private Database database;
  private Vertex alice, bob, charlie;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-varlen-path").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Use low-level API to avoid any Cypher CREATE issues
    database.transaction(() -> {
      alice = database.newVertex("Person").set("name", "Alice").save();
      bob = database.newVertex("Person").set("name", "Bob").save();
      charlie = database.newVertex("Person").set("name", "Charlie").save();

      ((MutableVertex) alice).newEdge("KNOWS", bob, true, (Object[]) null).save();
      ((MutableVertex) bob).newEdge("KNOWS", charlie, true, (Object[]) null).save();
    });
  }

  @AfterEach
  public void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  public void testNamedPathVariableLengthStoresPath() {
    // Test that path variable IS being stored (even if there are duplicates)
    // This verifies the fix: passing pathVariable instead of relVar to ExpandPathStep
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) " +
            "RETURN p AS path LIMIT 1");

    assertTrue(result.hasNext(), "Should have at least one result");
    final Result row = result.next();

    final Object pathObj = row.getProperty("path");
    assertNotNull(pathObj, "Path variable should not be null - this was the bug!");
    assertTrue(pathObj instanceof TraversalPath, "Path should be a TraversalPath");

    final TraversalPath path = (TraversalPath) pathObj;
    assertTrue(path.length() >= 1 && path.length() <= 2, "Path length should be 1 or 2");
    assertEquals("Alice", path.getStartVertex().get("name"));

    result.close();
  }

  @Test
  public void testNamedPathSingleHop() {
    // Single-hop paths should work correctly (no duplication bug)
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) " +
            "RETURN p AS path");

    assertTrue(result.hasNext());
    final Result row = result.next();

    final TraversalPath path = (TraversalPath) row.getProperty("path");
    assertNotNull(path);
    assertEquals(1, path.length());
    assertEquals("Alice", path.getStartVertex().get("name"));
    assertEquals("Bob", path.getEndVertex().get("name"));

    assertFalse(result.hasNext(), "Single-hop should return exactly 1 result");
    result.close();
  }
}
