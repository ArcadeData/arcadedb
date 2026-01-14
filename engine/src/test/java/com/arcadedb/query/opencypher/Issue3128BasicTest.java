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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic test for GitHub issue #3128 - verifies ID() function works in WHERE clauses.
 * This test focuses on the core functionality that was broken.
 */
class Issue3128BasicTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3128-basic").create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("in");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testIdFunctionInWhereClause() {
    // Create nodes
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Node {name: 'Node1'})");
      database.command("opencypher", "CREATE (b:Node {name: 'Node2'})");
    });

    // Get node IDs
    ResultSet result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Node1' RETURN ID(n) AS id");
    String node1Id = (String) result.next().getProperty("id");

    result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Node2' RETURN ID(n) AS id");
    String node2Id = (String) result.next().getProperty("id");

    System.out.println("Node1 ID: " + node1Id);
    System.out.println("Node2 ID: " + node2Id);

    // Test ID() function in WHERE clause with parameter - THIS WAS THE ORIGINAL ISSUE
    result = database.query("opencypher",
        "MATCH (a) WHERE ID(a) = $sourceId RETURN a",
        Map.of("sourceId", node1Id));

    int count = 0;
    while (result.hasNext()) {
      Result r = result.next();
      Vertex v = (Vertex) r.getProperty("a");
      System.out.println("Matched: " + v.get("name"));
      count++;
    }

    assertThat(count).isEqualTo(1);

    // Test with second node
    result = database.query("opencypher",
        "MATCH (b) WHERE ID(b) = $targetId RETURN b",
        Map.of("targetId", node2Id));

    count = 0;
    while (result.hasNext()) {
      Result r = result.next();
      Vertex v = (Vertex) r.getProperty("b");
      System.out.println("Matched: " + v.get("name"));
      count++;
    }

    assertThat(count).isEqualTo(1);
  }

  @Test
  void testIdFunctionWithMultipleMatches() {
    // Create nodes
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Node {name: 'Source'})");
      database.command("opencypher", "CREATE (b:Node {name: 'Target'})");
    });

    // Get node IDs
    ResultSet result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Source' RETURN ID(n) AS id");
    String sourceId = (String) result.next().getProperty("id");

    result = database.query("opencypher", "MATCH (n:Node) WHERE n.name = 'Target' RETURN ID(n) AS id");
    String targetId = (String) result.next().getProperty("id");

    // Test matching both nodes in a single query
    result = database.query("opencypher",
        "MATCH (a) WHERE ID(a) = $sourceId " +
        "MATCH (b) WHERE ID(b) = $targetId " +
        "RETURN a, b",
        Map.of("sourceId", sourceId, "targetId", targetId));

    assertThat(result.hasNext()).isTrue();
    Result r = result.next();
    Vertex a = (Vertex) r.getProperty("a");
    Vertex b = (Vertex) r.getProperty("b");

    assertThat(a.get("name")).isEqualTo("Source");
    assertThat(b.get("name")).isEqualTo("Target");
  }
}
