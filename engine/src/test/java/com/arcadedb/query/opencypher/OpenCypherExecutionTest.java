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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * End-to-end tests for query execution.
 * Phase 3: Tests for complete query execution from parsing to results.
 */
public class OpenCypherExecutionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-exec").create();
    setupTestData();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void setupTestData() {
    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_FOR");

    // Create test data
    database.transaction(() -> {
      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").set("age", 35).save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").set("age", 25).save();
      final MutableVertex acme = database.newVertex("Company").set("name", "Acme Corp").save();
      final MutableVertex techco = database.newVertex("Company").set("name", "TechCo").save();

      alice.newEdge("KNOWS", bob, true, (Object[]) null).save();
      bob.newEdge("KNOWS", charlie, true, (Object[]) null).save();
      alice.newEdge("WORKS_FOR", acme, true, (Object[]) null).save();
      bob.newEdge("WORKS_FOR", acme, true, (Object[]) null).save();
      charlie.newEdge("WORKS_FOR", techco, true, (Object[]) null).save();
    });
  }

  @Test
  void simpleMatchAllPersons() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(3);

    for (final Result r : results) {
      final Object vertex = r.toElement();
      assertThat(vertex).isInstanceOf(Vertex.class);
      final Vertex v = (Vertex) vertex;
      assertThat(v.getTypeName()).isEqualTo("Person");
    }
  }

  @Test
  void matchWithWhereFilter() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) WHERE n.age > 28 RETURN n");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return Alice (30) and Bob (35), not Charlie (25)
    assertThat(results).hasSize(2);

    for (final Result r : results) {
      final Vertex vertex = (Vertex) r.toElement();
      final int age = (int) vertex.get("age");
      assertThat(age).isGreaterThan(28);
    }
  }

  @Test
  void matchWithPropertyProjection() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(3);

    final List<String> names = new ArrayList<>();
    for (final Result r : results) {
      final Object nameObj = r.getProperty("n.name");
      assertThat(nameObj).isNotNull();
      names.add((String) nameObj);
    }

    assertThat(names).containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
  }

  @Test
  void matchAllCompanies() {
    final ResultSet result = database.query("opencypher", "MATCH (c:Company) RETURN c");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(2);

    for (final Result r : results) {
      final Vertex vertex = (Vertex) r.toElement();
      assertThat(vertex.getTypeName()).isEqualTo("Company");
    }
  }

  @Test
  void matchWithMultipleFilters() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) WHERE n.age > 25 RETURN n.name");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return Alice and Bob (age > 25)
    assertThat(results).hasSize(2);

    final List<String> names = new ArrayList<>();
    for (final Result r : results) {
      names.add((String) r.getProperty("n.name"));
    }

    assertThat(names).containsExactlyInAnyOrder("Alice", "Bob");
  }

  @Test
  void matchRelationshipPattern() {
    final ResultSet result = database.query("opencypher", "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should find: Alice->Bob, Bob->Charlie
    assertThat(results).hasSize(2);

    for (final Result r : results) {
      assertThat((Object) r.getProperty("a")).isInstanceOf(Vertex.class);
      assertThat((Object) r.getProperty("b")).isInstanceOf(Vertex.class);
      assertThat((Object) r.getProperty("r")).isNotNull();
    }
  }

  /**
   * Tests that RETURN clause preserves the column order specified in the query.
   * Issue: https://github.com/ArcadeData/arcadedb/issues/3475
   */
  @Test
  void returnColumnOrderPreserved() {
    database.getSchema().createEdgeType("spouseOf");

    database.transaction(() -> {
      final MutableVertex joseph = database.newVertex("Person").set("name", "Joseph").set("nameFamily", "Smith").set("nameGiven", "Joseph").save();
      final MutableVertex mary = database.newVertex("Person").set("name", "Mary").set("nameFamily", "Jones").set("nameGiven", "Mary").save();
      joseph.newEdge("spouseOf", mary, true, new Object[] { "marriageDate", "1990-06-15", "divorceDate", "2000-01-01" }).save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person)-[r:spouseOf]->(m:Person) WHERE n.nameFamily='Smith' AND n.nameGiven='Joseph' RETURN n, r.marriageDate, r.divorceDate, m");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);
    assertThat(results).hasSize(1);

    final Result row = results.get(0);
    final List<String> columnOrder = new ArrayList<>(row.getPropertyNames());

    // The columns must match the order specified in the RETURN clause: n, r.marriageDate, r.divorceDate, m
    assertThat(columnOrder).containsExactly("n", "r.marriageDate", "r.divorceDate", "m");
  }

  private List<Result> collectResults(final ResultSet resultSet) {
    final List<Result> results = new ArrayList<>();
    while (resultSet.hasNext()) {
      results.add(resultSet.next());
    }
    return results;
  }
}
