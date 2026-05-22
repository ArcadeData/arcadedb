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

/**
 * Regression test for issue #4282: a Cypher-style numeric id (the value returned by {@code id()} / {@code .asCypherRID()}) can be passed as the FROM target of
 * a SQL SELECT and is resolved back to the original record in O(1), exactly like a native {@code #bucketId:offset} RID literal.
 */
class Issue4282SelectByCypherRidTest {

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-issue4282-" + UUID.randomUUID()).create();
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
  void selectFromNamedParameterWithCypherId() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT FROM :id LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(aliceRid);
    }
  }

  @Test
  void selectFromPositionalParameterWithCypherId() {
    final RID bobRid = resolveRid("Bob");
    final long cypherId = cypherId("Bob");

    try (final ResultSet rs = database.query("sql", "SELECT FROM ? LIMIT 1", cypherId)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(bobRid);
    }
  }

  @Test
  void selectFromBareIntegerLiteralWithCypherId() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + cypherId + " LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(aliceRid);
    }
  }

  @Test
  void updateByBareIntegerLiteralWithCypherId() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql",
          "UPDATE " + cypherId + " SET tag = 'literal' RETURN AFTER @rid")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<RID>getProperty("@rid")).isEqualTo(aliceRid);
      }
    });

    try (final ResultSet rs = database.query("sql", "SELECT tag FROM " + cypherId + " LIMIT 1")) {
      assertThat(rs.next().<String>getProperty("tag")).isEqualTo("literal");
    }
  }

  // Non-regression: a bare integer must only be treated as a Cypher RID in a FROM target, never in a normal expression/projection.
  @Test
  void bareIntegerInProjectionStaysANumber() {
    try (final ResultSet rs = database.query("sql", "SELECT 30064771072 AS n, 2 + 3 AS sum")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(30064771072L);
      assertThat(row.<Number>getProperty("sum").longValue()).isEqualTo(5L);
    }
  }

  // Non-regression: the native RID literal forms (#bucket:offset and legacy bucket:offset) keep working.
  @Test
  void nativeRidLiteralFormsStillWork() {
    final RID aliceRid = resolveRid("Alice");

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + aliceRid + " LIMIT 1")) {
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(aliceRid);
    }
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM " + aliceRid.getBucketId() + ":" + aliceRid.getPosition() + " LIMIT 1")) {
      assertThat(rs.next().getIdentity().orElseThrow()).isEqualTo(aliceRid);
    }
  }

  @Test
  void cypherIdRoundTripMatchesCypherIdFunction() {
    // The id used by SELECT FROM must be the very same value the OpenCypher id() function produces.
    final long cypherId;
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name:'Charlie'}) RETURN id(p) AS ident")) {
      assertThat(rs.hasNext()).isTrue();
      cypherId = rs.next().<Number>getProperty("ident").longValue();
    }

    try (final ResultSet rs = database.query("sql", "SELECT name FROM :id LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Charlie");
    }
  }

  @Test
  void updateByCypherIdParameter() {
    final RID aliceRid = resolveRid("Alice");
    final long cypherId = cypherId("Alice");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql",
          "UPDATE :id SET tag = :tag RETURN AFTER @rid", Map.of("id", cypherId, "tag", "updated"))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<RID>getProperty("@rid")).isEqualTo(aliceRid);
      }
    });

    try (final ResultSet rs = database.query("sql", "SELECT tag FROM :id LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.next().<String>getProperty("tag")).isEqualTo("updated");
    }
  }

  @Test
  void selectFromCypherIdResolvedViaAsCypherRID() {
    // End-to-end: take the Cypher id produced by the SQL .asCypherRID() method and feed it straight back into a FROM target.
    final long cypherId;
    try (final ResultSet rs = database.query("sql", "SELECT @rid.asCypherRID() AS ident FROM Person WHERE name = 'Alice'")) {
      assertThat(rs.hasNext()).isTrue();
      cypherId = rs.next().<Number>getProperty("ident").longValue();
    }

    try (final ResultSet rs = database.query("sql", "SELECT name FROM :id LIMIT 1", Map.of("id", cypherId))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    }
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
