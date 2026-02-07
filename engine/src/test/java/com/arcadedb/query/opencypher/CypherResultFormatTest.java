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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.arcadedb.query.opencypher.executor.steps.FinalProjectionStep.PROJECTION_NAME_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Cypher RETURN clause produces correct result formats.
 * When returning a single node variable (e.g., RETURN n), the result should be the element
 * directly, not wrapped in {"n": element}.
 */
class CypherResultFormatTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypherresultformat").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void returnSingleNodeHasVariableColumn() {
    // MATCH (n) RETURN n should return result with column "n" containing the vertex
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(2);
    for (final Result r : results) {
      assertThat(r.getPropertyNames()).contains("n");
      assertThat((Object) r.getProperty("n")).isInstanceOf(Vertex.class);
    }
  }

  @Test
  void returnSingleEdgeHasVariableColumn() {
    // MATCH ()-[r]->() RETURN r should return result with column "r"
    final ResultSet result = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN r");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(1);
    assertThat(results.getFirst().getPropertyNames()).contains("r");
  }

  @Test
  void returnPropertyIsProjection() {
    // MATCH (n) RETURN n.name should return projections, not elements
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(2);
    for (final Result r : results) {
      assertThat(r.isProjection()).as("Property access result should be a projection").isTrue();
      assertThat((Object) r.getProperty("n.name")).isNotNull();
    }
  }

  @Test
  void returnMultipleColumnsIsProjection() {
    // MATCH (n) RETURN n, n.name should return projections with both columns
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n, n.name");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(2);
    for (final Result r : results) {
      assertThat(r.isProjection()).as("Multi-column result should be a projection").isTrue();
      assertThat(r.getPropertyNames()).contains("n", "n.name");
    }
  }

  @Test
  void returnNodeWithAlias() {
    // MATCH (n) RETURN n AS person — result should have column "person" with the vertex
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n AS person");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(2);
    for (final Result r : results) {
      assertThat(r.getPropertyNames()).contains("person");
      assertThat((Object) r.getProperty("person")).isInstanceOf(Vertex.class);
    }
  }

  @Test
  void returnCountIsProjection() {
    // MATCH (n) RETURN count(n) should return a projection with the count value
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN count(n)");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(1);
    assertThat(results.getFirst().isProjection()).as("Count result should be a projection").isTrue();
  }

  @Test
  void singleNodeHasProjectionNameMetadata() {
    // RETURN n should have _projectionName metadata for wire protocols
    // The result should have column "n" with the vertex as value
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n");
    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }

    assertThat(results).hasSize(2);
    for (final Result r : results) {
      assertThat(r.getPropertyNames()).contains("n");
      assertThat(r.getMetadata(PROJECTION_NAME_METADATA)).isEqualTo("n");
    }
  }
}
