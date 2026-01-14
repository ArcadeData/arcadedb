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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test range index usage in Cypher queries.
 * Verifies that range predicates (>, <, >=, <=) use LSM indexes for efficient scanning.
 */
public class CypherRangeIndexTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/cypherrangeindex").create();

    database.transaction(() -> {
      // Create Person vertex type with age property
      final Schema schema = database.getSchema();
      final VertexType personType = schema.createVertexType("Person");

      // Create index on age property (LSM index supports range scans)
      personType.createProperty("age", Integer.class);
      personType.createProperty("name", String.class);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Person", "age");

      // Insert test data using database API
      for (int i = 1; i <= 100; i++) {
        final MutableVertex person = database.newVertex("Person");
        person.set("name", "Person" + i);
        person.set("age", i);
        person.save();
      }
    });
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  public void testDataInserted() {
    // First verify data was inserted
    final ResultSet allResults = database.query("opencypher", "MATCH (p:Person) RETURN count(p) as count");
    assertThat(allResults.hasNext()).isTrue();
    final Result countResult = allResults.next();
    assertThat(countResult.<Long>getProperty("count")).isEqualTo(100L);
  }

  @Test
  public void testBasicQuery() {
    // Test a simple query to ensure basic matching works
    final ResultSet results = database.query("opencypher", "MATCH (p:Person) RETURN p.name, p.age LIMIT 5");
    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      assertThat((Object) result.getProperty("p.name")).isNotNull();
      assertThat((Object) result.getProperty("p.age")).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(5);
  }

  @Test
  public void testRangeGreaterThan() {
    // Query: age > 90 (should return 10 people)
    final ResultSet results = database.query("opencypher", "MATCH (p:Person) WHERE p.age > 90 RETURN p.name, p.age ORDER BY p.age");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final int age = ((Number) result.getProperty("p.age")).intValue();
      assertThat(age).isGreaterThan(90);
      count++;
    }
    assertThat(count).isEqualTo(10);
  }

  @Test
  public void testRangeLessThan() {
    // Query: age < 11 (should return 10 people)
    final ResultSet results = database.query("opencypher", "MATCH (p:Person) WHERE p.age < 11 RETURN p.name, p.age ORDER BY p.age");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final int age = ((Number) result.getProperty("p.age")).intValue();
      assertThat(age).isLessThan(11);
      count++;
    }
    assertThat(count).isEqualTo(10);
  }

  @Test
  public void testRangeBetween() {
    // Query: age >= 40 AND age <= 60 (should return 21 people: 40-60 inclusive)
    final ResultSet results = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age >= 40 AND p.age <= 60 RETURN p.name, p.age ORDER BY p.age");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final int age = ((Number) result.getProperty("p.age")).intValue();
      assertThat(age).isGreaterThanOrEqualTo(40);
      assertThat(age).isLessThanOrEqualTo(60);
      count++;
    }
    assertThat(count).isEqualTo(21);
  }

  @Test
  public void testRangeGreaterThanOrEqual() {
    // Query: age >= 95 (should return 6 people: 95-100)
    final ResultSet results = database.query("opencypher", "MATCH (p:Person) WHERE p.age >= 95 RETURN p.name, p.age ORDER BY p.age");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final int age = ((Number) result.getProperty("p.age")).intValue();
      assertThat(age).isGreaterThanOrEqualTo(95);
      count++;
    }
    assertThat(count).isEqualTo(6);
  }

  @Test
  public void testRangeLessThanOrEqual() {
    // Query: age <= 5 (should return 5 people: 1-5)
    final ResultSet results = database.query("opencypher", "MATCH (p:Person) WHERE p.age <= 5 RETURN p.name, p.age ORDER BY p.age");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final int age = ((Number) result.getProperty("p.age")).intValue();
      assertThat(age).isLessThanOrEqualTo(5);
      count++;
    }
    assertThat(count).isEqualTo(5);
  }

  @Test
  public void testRangeWithParameter() {
    // TODO: Range scan optimization not yet supported for parameterized queries
    // For now, this uses a full scan with filter instead of range scan
    final ResultSet results = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > $minAge RETURN p.name, p.age ORDER BY p.age",
        Map.of("minAge", 80));

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final int age = ((Number) result.getProperty("p.age")).intValue();
      assertThat(age).isGreaterThan(80);
      count++;
    }
    assertThat(count).isEqualTo(20);
  }

  @Test
  public void testExplainShowsRangeScan() {
    // Verify that EXPLAIN shows NodeIndexRangeScan operator
    final ResultSet results = database.query("opencypher",
        "EXPLAIN MATCH (p:Person) WHERE p.age > 50 AND p.age < 60 RETURN p");

    final StringBuilder plan = new StringBuilder();
    while (results.hasNext()) {
      plan.append(results.next().toJSON());
    }

    final String planStr = plan.toString();
    // Should contain NodeIndexRangeScan (not NodeByLabelScan)
    assertThat(planStr).contains("NodeIndexRangeScan");
    assertThat(planStr).contains("age");
  }
}
