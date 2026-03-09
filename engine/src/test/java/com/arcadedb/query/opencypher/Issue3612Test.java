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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for issue #3612: UNWIND $batch MATCH ... CREATE edge fails
 * when inline property filters reference UNWIND variables.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3612Test {
  private static Database database;
  private static final String DB_PATH = "./target/test-databases/issue-3612-test";

  @BeforeAll
  static void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.transaction(() -> {
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("id", Type.INTEGER);
      personType.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");
      database.getSchema().createEdgeType("KNOWS");
    });

    // Create 5 persons
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "CREATE (:Person {id: " + i + ", name: 'Person" + i + "'})");
    });
  }

  @AfterAll
  static void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Test: UNWIND with inline property filter on single MATCH with comma-separated patterns.
   * This is the exact pattern from the issue that fails.
   */
  @Test
  void testUnwindWithInlinePropertyFilterCommaPattern() {
    // Clean edges first
    database.command("opencypher", "MATCH ()-[k:KNOWS]->() DELETE k");

    final List<Map<String, Object>> batch = List.of(
        Map.of("src_id", 0, "dst_id", 1, "weight", 0.5),
        Map.of("src_id", 1, "dst_id", 2, "weight", 0.7),
        Map.of("src_id", 2, "dst_id", 3, "weight", 0.3)
    );

    // Use command() which handles its own transaction
    database.command("opencypher",
        "UNWIND $batch AS e " +
            "MATCH (a:Person {id: e.src_id}), (b:Person {id: e.dst_id}) " +
            "CREATE (a)-[:KNOWS {weight: e.weight}]->(b)",
        Map.of("batch", batch));

    // Verify edges were created
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(3L);
    }
  }

  /**
   * Test: UNWIND with WHERE clause (working pattern from the issue).
   * This should work and serves as the baseline.
   */
  @Test
  void testUnwindWithWhereClause() {
    // Clean edges first
    database.command("opencypher", "MATCH ()-[k:KNOWS]->() DELETE k");

    final List<Map<String, Object>> batch = List.of(
        Map.of("src_id", 0, "dst_id", 1, "weight", 0.5),
        Map.of("src_id", 1, "dst_id", 2, "weight", 0.7),
        Map.of("src_id", 2, "dst_id", 3, "weight", 0.3)
    );

    database.command("opencypher",
        "UNWIND $batch AS e " +
            "MATCH (a:Person) WHERE a.id = e.src_id " +
            "MATCH (b:Person) WHERE b.id = e.dst_id " +
            "CREATE (a)-[:KNOWS {weight: e.weight}]->(b)",
        Map.of("batch", batch));

    // Verify edges were created
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(3L);
    }
  }

  /**
   * Test: Single MATCH with inline property filter referencing UNWIND variable.
   * Simpler version of the failing pattern.
   */
  @Test
  void testUnwindWithSingleMatchInlineFilter() {
    final List<Map<String, Object>> batch = List.of(
        Map.of("id", 0),
        Map.of("id", 1),
        Map.of("id", 2)
    );

    try (final ResultSet rs = database.query("opencypher",
        "UNWIND $batch AS e " +
            "MATCH (a:Person {id: e.id}) " +
            "RETURN a.name AS name ORDER BY name",
        Map.of("batch", batch))) {
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).containsExactly("Person0", "Person1", "Person2");
    }
  }

  /**
   * Test: Inline property filter with literal value works (baseline).
   */
  @Test
  void testInlinePropertyFilterWithLiteral() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {id: 0}) RETURN a.name AS name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Person0");
    }
  }

  /**
   * Test: Inline property filter with parameter works (baseline).
   */
  @Test
  void testInlinePropertyFilterWithParameter() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {id: $id}) RETURN a.name AS name",
        Map.of("id", 0))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Person0");
    }
  }

  /**
   * Test: UNWIND with inline property filter and simple list (not map).
   */
  @Test
  void testUnwindSimpleListWithInlineFilter() {
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [0, 1, 2] AS id " +
            "MATCH (a:Person {id: id}) " +
            "RETURN a.name AS name ORDER BY name")) {
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).containsExactly("Person0", "Person1", "Person2");
    }
  }
}
