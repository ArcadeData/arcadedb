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
package com.arcadedb.query.sql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the SQL {@code cypherRID()} function (issue #4282): the explicit inverse of {@code .asCypherRID()} / Cypher {@code id()}. It decodes a numeric Cypher
 * id back to a native RID, usable in projections and - via function-as-target - as the source of a SELECT/UPDATE/DELETE, resolved in O(1).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRIDFunctionTest {

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-cypherrid-fn-" + UUID.randomUUID()).create();
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").save();
      database.newVertex("Person").set("name", "Bob").save();
      database.newVertex("Person").set("name", "Charlie").save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void functionReturnsNativeRid() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT cypherRID(:id) AS rid", Map.of("id", cypherId))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<RID>getProperty("rid")).isEqualTo(aliceRid);
    }
  }

  @Test
  void selectFromFunctionWithNamedParameter() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT name FROM cypherRID(:id) LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    }
  }

  @Test
  void selectFromFunctionWithBareIntegerLiteral() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT FROM cypherRID(" + cypherId + ") LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(aliceRid);
    }
  }

  @Test
  void updateByFunctionTarget() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql",
          "UPDATE cypherRID(:id) SET tag = 'updated' RETURN AFTER @rid", Map.of("id", cypherId))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<RID>getProperty("@rid")).isEqualTo(aliceRid);
      }
    });

    try (final ResultSet rs = database.query("sql", "SELECT tag FROM cypherRID(:id) LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.next().<String>getProperty("tag")).isEqualTo("updated");
    }
  }

  @Test
  void deleteByFunctionTarget() {
    final long cypherId = cypherId("Bob");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM cypherRID(" + cypherId + ")")) {
        assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
      }
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = 'Bob'")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void roundTripWithCypherIdFunction() {
    final long cypherId;
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name:'Charlie'}) RETURN id(p) AS ident")) {
      cypherId = rs.next().<Number>getProperty("ident").longValue();
    }

    try (final ResultSet rs = database.query("sql", "SELECT name FROM cypherRID(:id) LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Charlie");
    }
  }

  @Test
  void roundTripWithAsCypherRIDMethod() {
    final long cypherId;
    try (final ResultSet rs = database.query("sql", "SELECT @rid.asCypherRID() AS ident FROM Person WHERE name = 'Alice'")) {
      cypherId = rs.next().<Number>getProperty("ident").longValue();
    }

    try (final ResultSet rs = database.query("sql", "SELECT name FROM cypherRID(:id) LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    }
  }

  // Non-regression: a bare integer in a FROM target is NOT auto-converted; it must go through cypherRID() explicitly.
  @Test
  void bareIntegerInProjectionStaysANumber() {
    try (final ResultSet rs = database.query("sql", "SELECT 30064771072 AS n, 2 + 3 AS sum")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(30064771072L);
      assertThat(row.<Number>getProperty("sum").longValue()).isEqualTo(5L);
    }
  }

  // Non-regression: native RID literal forms still work as FROM targets.
  @Test
  void nativeRidLiteralFormsStillWork() {
    final RID aliceRid = resolveRid("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + aliceRid + " LIMIT 1")) {
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(aliceRid);
    }
  }

  @Test
  void negativeCypherIdIsRejected() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT cypherRID(-5) AS rid")) {
        rs.next().getProperty("rid");
      }
    }).hasMessageContaining("negative");
  }

  private RID resolveRid(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = ?", name)) {
      return ((Vertex) rs.next().getElement().orElseThrow()).getIdentity();
    }
  }

  private long cypherId(final String name) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name:$n}) RETURN id(p) AS ident", Map.of("n", name))) {
      return rs.next().<Number>getProperty("ident").longValue();
    }
  }
}
