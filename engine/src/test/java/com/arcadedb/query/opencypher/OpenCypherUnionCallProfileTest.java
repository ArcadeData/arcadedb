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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for UNION, CALL, and PROFILE features.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OpenCypherUnionCallProfileTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-union-call-profile").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ============================================================
  // UNION Tests
  // ============================================================

  @Test
  void testUnionBasic() {
    // Setup: Create Person and Company types with data
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
      database.command("opencypher", "CREATE (n:Company {name: 'ArcadeDB'})");
      database.command("opencypher", "CREATE (n:Company {name: 'TechCorp'})");
    });

    // Test UNION (should remove duplicates)
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN n.name AS name " +
        "UNION " +
        "MATCH (n:Company) RETURN n.name AS name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      names.add(row.getProperty("name"));
    }

    assertThat(names).hasSize(4);
    assertThat(names).contains("Alice", "Bob", "ArcadeDB", "TechCorp");
  }

  @Test
  void testUnionDeduplicates() {
    // Setup: Create data with duplicates
    database.getSchema().createVertexType("Type1");
    database.getSchema().createVertexType("Type2");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Type1 {name: 'Same'})");
      database.command("opencypher", "CREATE (n:Type2 {name: 'Same'})");
    });

    // Test UNION removes duplicates
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Type1) RETURN n.name AS name " +
        "UNION " +
        "MATCH (n:Type2) RETURN n.name AS name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      names.add(row.getProperty("name"));
    }

    assertThat(names).hasSize(1);
    assertThat(names.get(0)).isEqualTo("Same");
  }

  @Test
  void testUnionAllKeepsDuplicates() {
    // Setup: Create data with duplicates
    database.getSchema().createVertexType("Type1");
    database.getSchema().createVertexType("Type2");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Type1 {name: 'Same'})");
      database.command("opencypher", "CREATE (n:Type2 {name: 'Same'})");
    });

    // Test UNION ALL keeps duplicates
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Type1) RETURN n.name AS name " +
        "UNION ALL " +
        "MATCH (n:Type2) RETURN n.name AS name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      names.add(row.getProperty("name"));
    }

    assertThat(names).hasSize(2);
    assertThat(names).containsExactly("Same", "Same");
  }

  @Test
  void testMultipleUnions() {
    // Setup
    database.getSchema().createVertexType("TypeA");
    database.getSchema().createVertexType("TypeB");
    database.getSchema().createVertexType("TypeC");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:TypeA {name: 'A'})");
      database.command("opencypher", "CREATE (n:TypeB {name: 'B'})");
      database.command("opencypher", "CREATE (n:TypeC {name: 'C'})");
    });

    // Test multiple UNIONs
    final ResultSet result = database.query("opencypher",
        "MATCH (n:TypeA) RETURN n.name AS name " +
        "UNION " +
        "MATCH (n:TypeB) RETURN n.name AS name " +
        "UNION " +
        "MATCH (n:TypeC) RETURN n.name AS name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      names.add(row.getProperty("name"));
    }

    assertThat(names).hasSize(3);
    assertThat(names).contains("A", "B", "C");
  }

  // ============================================================
  // CALL Tests
  // ============================================================

  @Test
  void testCallDbLabels() {
    // Setup: Create some vertex types
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createVertexType("City");

    // Create some vertices to make the types active
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (n:Company {name: 'ArcadeDB'})");
    });

    // Test CALL db.labels()
    final ResultSet result = database.query("opencypher", "CALL db.labels()");

    final Set<String> labels = new HashSet<>();
    while (result.hasNext()) {
      final Result row = result.next();
      labels.add(row.getProperty("label"));
    }

    assertThat(labels).contains("Person", "Company", "City");
  }

  @Test
  void testCallDbRelationshipTypes() {
    // Setup: Create vertex and edge types
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_AT");

    // Test CALL db.relationshipTypes()
    final ResultSet result = database.query("opencypher", "CALL db.relationshipTypes()");

    final Set<String> types = new HashSet<>();
    while (result.hasNext()) {
      final Result row = result.next();
      types.add(row.getProperty("relationshipType"));
    }

    assertThat(types).contains("KNOWS", "WORKS_AT");
  }

  @Test
  void testCallDbPropertyKeys() {
    // Setup: Create types with properties
    final var personType = database.getSchema().createVertexType("Person");
    personType.createProperty("name", String.class);
    personType.createProperty("age", Integer.class);

    // Test CALL db.propertyKeys()
    final ResultSet result = database.query("opencypher", "CALL db.propertyKeys()");

    final Set<String> keys = new HashSet<>();
    while (result.hasNext()) {
      final Result row = result.next();
      keys.add(row.getProperty("propertyKey"));
    }

    assertThat(keys).contains("name", "age");
  }

  @Test
  void testCallCustomSQLFunction() {
    // Define a custom SQL function
    database.command("sql", "DEFINE FUNCTION math.add \"SELECT :a + :b AS result\" PARAMETERS [a,b] LANGUAGE sql");

    // Test calling the custom function via Cypher CALL
    final ResultSet result = database.query("opencypher", "CALL math.add(3, 5)");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    // The SQL function returns a scalar which gets wrapped with "value" property
    final Object value = row.getProperty("value");
    assertThat(value).isNotNull();
    assertThat(((Number) value).intValue()).isEqualTo(8);
  }

  @Test
  void testCallCustomFunctionWithStringParam() {
    // Define a custom SQL function that returns input
    database.command("sql", "DEFINE FUNCTION my.echo \"SELECT :input AS output\" PARAMETERS [input] LANGUAGE sql");

    // Test calling with string parameter
    final ResultSet result = database.query("opencypher", "CALL my.echo('Hello World')");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    // The SQL function returns a scalar which gets wrapped with "value" property
    assertThat((Object) row.getProperty("value")).isEqualTo("Hello World");
  }

  // ============================================================
  // PROFILE Tests
  // ============================================================

  @Test
  void testProfileBasic() {
    // Setup
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });

    // Test PROFILE
    final ResultSet result = database.query("opencypher", "PROFILE MATCH (n:Person) RETURN n.name");

    // First result should be profile info
    assertThat(result.hasNext()).isTrue();
    final Result profileResult = result.next();
    final String profile = profileResult.getProperty("profile");
    assertThat(profile).isNotNull();
    assertThat(profile).contains("OpenCypher Query Profile");
    assertThat(profile).contains("Execution Time");
    assertThat(profile).contains("Rows Returned");

    // Following results are the actual query results
    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void testProfileShowsRowCount() {
    // Setup
    database.getSchema().createVertexType("Item");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.command("opencypher", "CREATE (n:Item {value: " + i + "})");
      }
    });

    // Test PROFILE shows correct row count
    final ResultSet result = database.query("opencypher", "PROFILE MATCH (n:Item) RETURN n.value");

    assertThat(result.hasNext()).isTrue();
    final Result profileResult = result.next();
    final String profile = profileResult.getProperty("profile");
    assertThat(profile).contains("Rows Returned: 5");
  }

  @Test
  void testExplainDoesNotExecute() {
    // Setup
    database.getSchema().createVertexType("Counter");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Counter {count: 0})");
    });

    // EXPLAIN should not execute the query, just show plan
    final ResultSet result = database.query("opencypher", "EXPLAIN MATCH (n:Counter) RETURN n.count");

    assertThat(result.hasNext()).isTrue();
    final Result explainResult = result.next();
    final String plan = explainResult.getProperty("plan");
    assertThat(plan).isNotNull();
    assertThat(plan).contains("OpenCypher Native Execution Plan");

    // Should only have one result (the plan)
    assertThat(result.hasNext()).isFalse();
  }
}
