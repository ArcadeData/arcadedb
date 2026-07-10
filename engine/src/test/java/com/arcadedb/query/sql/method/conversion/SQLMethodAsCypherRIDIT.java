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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the {@code .asCypherRID()} SQL method: verifies that the value it returns from a SQL projection matches what the native OpenCypher {@code id()} function returns for the same vertex (issue #4269).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLMethodAsCypherRIDIT {

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-ascypherrid-" + UUID.randomUUID()).create();
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
  void asCypherRIDFromSqlMatchesCypherIdFunction() {
    // Pick a vertex and compute its Cypher id via Cypher.
    final RID aliceRid;
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name:'Alice'}) RETURN p AS p")) {
      assertThat(rs.hasNext()).isTrue();
      aliceRid = ((Vertex) rs.next().getProperty("p")).getIdentity();
    }

    final long cypherIdFromCypher;
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name:'Alice'}) RETURN id(p) AS ident")) {
      assertThat(rs.hasNext()).isTrue();
      cypherIdFromCypher = rs.next().<Number>getProperty("ident").longValue();
    }

    // Same id computed from SQL via the new asCypherRID() method.
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid.asCypherRID() AS ident FROM Person WHERE name = 'Alice'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("ident")).isInstanceOf(Number.class);
      assertThat(row.<Number>getProperty("ident").longValue()).isEqualTo(cypherIdFromCypher);
    }

    // Sanity: encoded value matches the RID we resolved above.
    assertThat(cypherIdFromCypher).isEqualTo(IdFunction.encodeRidAsLong(aliceRid));
  }

  @Test
  void asCypherRIDOnStringLiteralWorks() {
    try (final ResultSet rs = database.query("sql",
        "SELECT '#10:10'.asCypherRID() AS ident")) {
      assertThat(rs.hasNext()).isTrue();
      final long encoded = rs.next().<Number>getProperty("ident").longValue();
      assertThat(encoded).isEqualTo(IdFunction.encodeRidAsLong(new RID(10, 10)));
    }
  }

  @Test
  void asCypherRIDOnNullReturnsNull() {
    try (final ResultSet rs = database.query("sql", "SELECT null.asCypherRID() AS ident")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("ident")).isNull();
    }
  }

  // Issue #4269: the use case from the bug report - method chained after a graph traversal returns a Cypher-compatible Long.
  @Test
  void asCypherRIDChainedAfterTraversalMatchesCypherId() {
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      final Vertex alice = (Vertex) database.query("sql", "SELECT FROM Person WHERE name = 'Alice'").next().getElement().get();
      final Vertex bob = (Vertex) database.query("sql", "SELECT FROM Person WHERE name = 'Bob'").next().getElement().get();
      alice.newEdge("KNOWS", bob).save();
    });

    final long bobIdFromCypher;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:Person {name:'Alice'})-[:KNOWS]->(b:Person) RETURN id(b) AS ident")) {
      assertThat(rs.hasNext()).isTrue();
      bobIdFromCypher = rs.next().<Number>getProperty("ident").longValue();
    }

    try (final ResultSet rs = database.query("sql",
        "SELECT out('KNOWS')[0].asCypherRID() AS ident FROM Person WHERE name = 'Alice'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("ident")).isInstanceOf(Number.class);
      assertThat(row.<Number>getProperty("ident").longValue()).isEqualTo(bobIdFromCypher);
    }
  }
}
