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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


;

/**
 * Tests for named paths with variable-length relationships.
 * Note: Variable-length path queries currently have a duplication bug (pre-existing issue).
 * These tests verify that path variables ARE being stored correctly.
 */
public class OpenCypherVariableLengthPathTest {
  private Database database;
  private Vertex alice, bob, charlie;

  @BeforeEach
  void setup() {
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
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void namedPathVariableLengthStoresPath() {
    // Test that path variable IS being stored (even if there are duplicates)
    // This verifies the fix: passing pathVariable instead of relVar to ExpandPathStep
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) " +
            "RETURN p AS path LIMIT 1");

    assertThat(result.hasNext()).as("Should have at least one result").isTrue();
    final Result row = result.next();

    final Object pathObj = row.getProperty("path");
    assertThat(pathObj).as("Path variable should not be null - this was the bug!").isNotNull();
    assertThat(pathObj).as("Path should be a TraversalPath").isInstanceOf(TraversalPath.class);

    final TraversalPath path = (TraversalPath) pathObj;
    assertThat(path.length() >= 1 && path.length() <= 2).as("Path length should be 1 or 2").isTrue();
    assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");

    result.close();
  }

  @Test
  void namedPathSingleHop() {
    // Single-hop paths should work correctly (no duplication bug)
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) " +
            "RETURN p AS path");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    final TraversalPath path = (TraversalPath) row.getProperty("path");
    assertThat(path).isNotNull();
    assertThat(path.length()).isEqualTo(1);
    assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");
    assertThat(path.getEndVertex().get("name")).isEqualTo("Bob");

    assertThat(result.hasNext()).as("Single-hop should return exactly 1 result").isFalse();
    result.close();
  }

  @Test
  void vlpPropertyPredicate() {
    // Create a separate database for this test with the TCK scenario
    final Database db2 = new DatabaseFactory("./target/databases/testopencypher-vlp-prop").create();
    try {
      db2.transaction(() -> {
        db2.command("opencypher",
            "CREATE (a:Artist:A), (b:Artist:B), (c:Artist:C) " +
                "CREATE (a)-[:WORKED_WITH {year: 1987}]->(b), " +
                "(b)-[:WORKED_WITH {year: 1988}]->(c)");
      });

      db2.transaction(() -> {
        // First: check all artists exist
        final ResultSet rs1 = db2.command("opencypher", "MATCH (n:Artist) RETURN n");
        int count = 0;
        while (rs1.hasNext()) {
          rs1.next();
          count++;
        }
        assertThat(count).as("Should have 3 Artist nodes").isEqualTo(3);

        // Second: VLP without property filter
        final ResultSet rs2 = db2.command("opencypher", "MATCH (a:Artist)-[:WORKED_WITH*]->(b) RETURN a, b");
        int count2 = 0;
        while (rs2.hasNext()) {
          rs2.next();
          count2++;
        }
        assertThat(count2).as("VLP without property filter should find paths").isGreaterThan(0);

        // Third: VLP WITHOUT target label filter to isolate the issue
        final ResultSet rs3a = db2.command("opencypher",
            "MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b) RETURN a, b");
        int count3a = 0;
        while (rs3a.hasNext()) {
          rs3a.next();
          count3a++;
        }

        // Fourth: VLP WITH target label only (no property filter)
        final ResultSet rs3b = db2.command("opencypher",
            "MATCH (a:Artist)-[:WORKED_WITH*]->(b:Artist) RETURN a, b");
        int count3b = 0;
        while (rs3b.hasNext()) {
          rs3b.next();
          count3b++;
        }

        // Fifth: VLP WITH property filter AND target label
        final ResultSet rs3 = db2.command("opencypher",
            "MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN a, b");
        int count3 = 0;
        while (rs3.hasNext()) {
          final Result r = rs3.next();
          count3++;
        }
        assertThat(count3a).as("VLP with property filter {year: 1988} without target label should find results").isGreaterThan(0);
        assertThat(count3b).as("VLP with target label :Artist but no property filter should find results").isGreaterThan(0);
        assertThat(count3).as("VLP with property filter {year: 1988} AND target label should find 1 result").isEqualTo(1);
      });
    } finally {
      db2.drop();
    }
  }
}
