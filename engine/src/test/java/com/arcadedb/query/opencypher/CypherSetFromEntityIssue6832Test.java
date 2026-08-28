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
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6832: {@code SET a = b} and {@code SET a += b}, where the right-hand side is a node or
 * a relationship rather than a literal map, were silent no-ops. Neo4j copies the source entity's properties onto the
 * target, and a right-hand side that is neither an entity nor a map is a type error rather than a discarded write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSetFromEntityIssue6832Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypher-6832").create();
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createEdgeType("REL");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void replaceFromNodeCopiesEveryProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, own: 'gone'}), (:B {id: 2, name: 'x', age: 7})"));

    database.transaction(() -> database.command("opencypher", "MATCH (a:A {id: 1}), (b:B {id: 2}) SET a = b"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.name AS name, a.age AS age, a.id AS id, a.own AS own");
    final var row = rs.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("x");
    assertThat(row.<Number>getProperty("age").intValue()).isEqualTo(7);
    // "=" replaces: the source's id wins and the target's own properties are dropped.
    assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(2);
    assertThat(row.<Object>getProperty("own")).isNull();
  }

  @Test
  void replaceFromNodeCountsPropertiesSet() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, own: 'gone'}), (:B {id: 2, name: 'x', age: 7})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "MATCH (a:A {id: 1}), (b:B {id: 2}) SET a = b");
      while (rs.hasNext())
        rs.next();
      final QueryStatistics stats = rs.getStatistics().orElseThrow();
      // 3 written (id, name, age) + 1 removed (own)
      assertThat(stats.getPropertiesSet()).isEqualTo(4);
    });
  }

  @Test
  void mergeFromNodeKeepsTargetOwnProperties() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, own: 'kept'}), (:B {id: 2, name: 'x'})"));

    database.transaction(() -> database.command("opencypher", "MATCH (a:A {id: 1}), (b:B {id: 2}) SET a += b"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.name AS name, a.id AS id, a.own AS own");
    final var row = rs.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("x");
    assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(2);
    assertThat(row.<String>getProperty("own")).isEqualTo("kept");
  }

  @Test
  void mergeFromRelationshipCopiesRelationshipProperties() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A {id: 1})-[:REL {since: 1999, kind: 'friend'}]->(b:B {id: 2})"));

    database.transaction(() -> database.command("opencypher", "MATCH (a:A)-[r:REL]->(b:B) SET a += r"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.since AS since, a.kind AS kind");
    final var row = rs.next();
    assertThat(row.<Number>getProperty("since").intValue()).isEqualTo(1999);
    assertThat(row.<String>getProperty("kind")).isEqualTo("friend");
  }

  @Test
  void replaceFromScalarIsATypeError() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a = 5")))
        .rootCause()
        .hasMessageContaining("TypeError");
  }

  @Test
  void mergeFromScalarIsATypeError() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a += 'nope'")))
        .rootCause()
        .hasMessageContaining("TypeError");
  }
}
