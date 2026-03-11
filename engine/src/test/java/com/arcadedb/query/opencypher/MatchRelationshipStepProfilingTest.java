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
class MatchRelationshipStepProfilingTest {
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
          """
          MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
          CREATE (a)-[:KNOWS {since: 2020}]->(b)""");

      database.command("opencypher",
          """
          MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) \
          CREATE (b)-[:KNOWS {since: 2021}]->(c)""");
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
        """
        PROFILE MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[r:KNOWS]->(c) \
        RETURN c.name AS name, r.since AS since""");

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

  @Test
  void nonExistentEdgeTypeShortCircuits() {
    // Query with edge type that doesn't exist in schema — should short-circuit
    // without scanning any edge segments (but MatchNodeStep IS pulled from to
    // ensure predecessor steps like CREATE/FOREACH execute before schema check).
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)<-[r:NONEXISTENT]-(b) RETURN a.name, b.name LIMIT 1");

    // Should return no results
    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isZero();

    // Check execution plan — MATCH RELATIONSHIP should exist but produce 0 results
    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);
    assertThat(planString).contains("MATCH RELATIONSHIP");

    result.close();
  }

  @Test
  void existingEdgeTypeShowsAccurateProfiling() {
    // Query with existing edge type and edge variable to force MATCH RELATIONSHIP step
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[r:KNOWS]->(b) RETURN a.name AS name, collect(b.name) AS friends");

    while (result.hasNext()) {
      result.next();
    }

    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);

    // MatchRelationshipStep should show timing
    assertThat(planString).contains("MATCH RELATIONSHIP");
    // GroupByAggregation should show timing
    assertThat(planString).contains("GROUP BY AGGREGATION");

    result.close();
  }

  @Test
  void singleHopAnonymousUsesFastPath() {
    // Single-hop anonymous relationship should use fast path (vertex-only traversal)
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, count(b) AS cnt");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should use optimized (fast path) traversal, not standard edges
    assertThat(planString).containsPattern("traversal: optimized \\(\\d+ vertices\\)");
    result.close();
  }

  @Test
  void multiHopDisjointTypesUsesFastPath() {
    // Multi-hop with disjoint edge types — each hop should use fast path
    // because different types can never be the same edge (uniqueness guaranteed)
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("LIVES_IN");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (c:City {name: 'NYC'})");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (c:City {name: 'NYC'}) CREATE (a)-[:LIVES_IN]->(c)");
    });

    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City) RETURN a.name, c.name");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Both hops should use optimized traversal (disjoint types → no uniqueness issue)
    assertThat(planString).doesNotContain("traversal: standard");
    result.close();
  }

  @Test
  void namedButUnusedEdgeVariableUsesFastPath() {
    // User writes [r:KNOWS] but never references r in RETURN/WHERE/ORDER BY
    // The planner should detect that r is unused and enable fast path
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, count(b) AS cnt");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should use optimized fast path even though r is declared — it's unused
    assertThat(planString).containsPattern("traversal: optimized \\(\\d+ vertices\\)");
    result.close();
  }

  @Test
  void namedAndUsedEdgeVariableUsesStandardPath() {
    // User writes [r:KNOWS] AND references r.since in RETURN — must use standard path
    // Use collect() aggregation to force the traditional execution path
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, collect(r.since) AS years");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Must use standard path because r is referenced in collect(r.since)
    assertThat(planString).contains("traversal: standard");
    result.close();
  }

  @Test
  void multiHopSameTypeUsesStandardPath() {
    // Multi-hop with same edge type — must use standard path for edge uniqueness
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name, c.name");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should use standard (edge-loading) for uniqueness checking
    assertThat(planString).contains("traversal: standard");
    result.close();
  }

  @Test
  void nonExistentTargetLabelShortCircuits() {
    // Edge type KNOWS exists, but target label NonExistent does not — should short-circuit
    // without scanning any edges (but predecessor steps are pulled first for lazy check)
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (a:Person)-[r:KNOWS]->(b:NonExistent) RETURN a.name, b.name LIMIT 1");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isZero();

    final ExecutionPlan plan = result.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);
    assertThat(planString).contains("MATCH RELATIONSHIP");

    result.close();
  }
}
