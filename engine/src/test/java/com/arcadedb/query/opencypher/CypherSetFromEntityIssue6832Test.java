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

import java.util.List;

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

  /**
   * The map forms used to be the way around the property-value type check that the dot form applies: only
   * applyPropertySet validated, so SET n = {x: {y: 1}} stored a map where SET n.x = {y: 1} raised. Neo4j refuses both
   * with "Property values can only be of primitive types or arrays thereof".
   */
  @Test
  void replaceMapRejectsANestedMapValue() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a = {x: {y: 1}}")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void mergeMapRejectsANestedMapValue() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a += {x: {y: 1}}")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void mergeMapRejectsAListOfMapsValue() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a += {x: [{y: 1}]}")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void mergeMapStillAcceptsAListOfScalars() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})"));

    database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a += {tags: ['x', 'y']}"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.tags AS tags");
    assertThat(rs.next().<List<Object>>getProperty("tags")).containsExactly("x", "y");
  }

  /** Copying from an entity onto itself must be a no-op, not a self-inflicted wipe: the replace form clears the
   *  target before reading the source, and a MutableDocument hands out a live view of its own property map. */
  @Test
  void replaceFromItselfKeepsEveryProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, name: 'keep'})"));

    database.transaction(() -> database.command("opencypher", "MATCH (a:A) SET a = a"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.id AS id, a.name AS name");
    final var row = rs.next();
    assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(1);
    assertThat(row.<String>getProperty("name")).isEqualTo("keep");
  }

  /**
   * SET is a simultaneous assignment: every read observes the pre-clause state (#5190). The entity right-hand side is
   * a read like any other, so an earlier item of the same clause must not be visible to it.
   */
  @Test
  void replaceFromNodeReadsThePreClauseStateOfTheSource() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:B {id: 2, name: 'old'})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:A), (b:B) SET b.name = 'new', a = b"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.name AS name");
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("old");

    final ResultSet source = database.query("opencypher", "MATCH (b:B) RETURN b.name AS name");
    assertThat(source.next().<String>getProperty("name")).isEqualTo("new");
  }

  @Test
  void mergeFromNodeReadsThePreClauseStateOfTheSource() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:B {id: 2, name: 'old'})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:A), (b:B) SET b.name = 'new', a += b"));

    final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.name AS name");
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("old");
  }
}
