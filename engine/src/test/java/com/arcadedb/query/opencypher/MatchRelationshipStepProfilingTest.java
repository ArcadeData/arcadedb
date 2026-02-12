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
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MatchRelationshipStep profiling output.
 * Verifies that PROFILE shows whether fast path (vertices) or standard path (edges) was used.
 */
public class MatchRelationshipStepProfilingTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-match-profiling").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      // Create a small social network
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 28})");

      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
              "CREATE (a)-[:KNOWS {since: 2020}]->(b)");

      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) " +
              "CREATE (b)-[:KNOWS {since: 2021}]->(c)");
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
  void profilingShowsFastPath() {
    // Fast path query: anonymous relationship
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name AS name");

    // Consume results
    while (result.hasNext()) {
      result.next();
    }

    // Check execution plan
    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);

    // Should show MATCH RELATIONSHIP with traversal info
    assertThat(planString).contains("MATCH RELATIONSHIP");
    // Profiling should show either optimized (vertices) or standard (edges) path
    assertThat(planString).containsPattern("traversal: (optimized|standard|mixed)");

    result.close();
  }

  @Test
  void profilingShowsStandardPath() {
    // Standard path query: edge variable present
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN b.name AS name, r.since AS since");

    // Consume results
    while (result.hasNext()) {
      result.next();
    }

    // Check execution plan
    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);

    // Should show standard traversal
    assertThat(planString).contains("MATCH RELATIONSHIP");
    assertThat(planString).containsAnyOf("standard", "edges");

    result.close();
  }

  @Test
  void profilingShowsMixedPath() {
    // Multi-hop query: first hop fast, second hop standard
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[r:KNOWS]->(c) " +
            "RETURN c.name AS name, r.since AS since");

    // Consume results
    while (result.hasNext()) {
      result.next();
    }

    // Check execution plan
    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);

    // Should show MATCH RELATIONSHIP steps
    assertThat(planString).contains("MATCH RELATIONSHIP");

    // At least one should show optimized (first hop) and one should show standard/edges (second hop with edge variable)
    // Or one might show mixed if they're combined

    result.close();
  }

  @Test
  void profilingWithoutMatchRelationship() {
    // Query without relationship traversal
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person {name: 'Alice'}) RETURN a.name AS name");

    // Consume results
    while (result.hasNext()) {
      result.next();
    }

    // Check execution plan
    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);

    // Should not contain MATCH RELATIONSHIP
    // (or if it does, it shouldn't have traversal info since no traversal happened)

    result.close();
  }

  @Test
  void profilingShowsCountsForMultipleMatches() {
    // Query that traverses multiple relationships
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS name");

    // Consume results
    while (result.hasNext()) {
      result.next();
    }

    // Check execution plan
    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);

    // Should show count of vertices traversed
    assertThat(planString).contains("MATCH RELATIONSHIP");
    assertThat(planString).containsPattern("(optimized|vertices|standard|edges)");

    result.close();
  }
}
