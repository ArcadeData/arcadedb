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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test COLLECT { ... } subquery expression in Cypher queries.
 * See <a href="https://github.com/ArcadeData/arcadedb/issues/3957">issue #3957</a>.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCollectSubqueryTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphercollect").create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");

      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
      database.newVertex("Person").set("name", "Charlie").save();

      alice.newEdge("KNOWS", bob, new Object[0]).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void collectCorrelatedFriendNames() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) \
        RETURN p.name AS person, \
               COLLECT { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name } AS friendNames \
        ORDER BY person""");

    final List<String> names = new ArrayList<>();
    while (results.hasNext()) {
      final Result r = results.next();
      final String person = r.getProperty("person");
      names.add(person);
      final Object friendNames = r.getProperty("friendNames");
      assertThat(friendNames).isInstanceOf(List.class);

      @SuppressWarnings("unchecked") final List<Object> list = (List<Object>) friendNames;
      switch (person) {
      case "Alice":
        assertThat(list).containsExactly("Bob");
        break;
      case "Bob":
      case "Charlie":
        assertThat(list).isEmpty();
        break;
      default:
        throw new AssertionError("Unexpected person: " + person);
      }
    }
    assertThat(names).isEqualTo(List.of("Alice", "Bob", "Charlie"));
  }

  @Test
  void collectAliceFriends() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person {name:'Alice'}) \
        RETURN COLLECT { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name } AS names""");

    assertThat(results.hasNext()).isTrue();
    final Result r = results.next();
    final Object names = r.getProperty("names");
    assertThat(names).isInstanceOf(List.class);
    @SuppressWarnings("unchecked") final List<Object> list = (List<Object>) names;
    assertThat(list).containsExactly("Bob");
    assertThat(results.hasNext()).isFalse();
  }

  @Test
  void collectEmptyResult() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person {name:'Charlie'}) \
        RETURN COLLECT { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name } AS names""");

    assertThat(results.hasNext()).isTrue();
    final Result r = results.next();
    final Object names = r.getProperty("names");
    assertThat(names).isInstanceOf(List.class);
    @SuppressWarnings("unchecked") final List<Object> list = (List<Object>) names;
    assertThat(list).isEqualTo(Collections.emptyList());
    assertThat(results.hasNext()).isFalse();
  }

  @Test
  void collectSubqueryWithWhereFilter() {
    // Add an extra KNOWS edge so Alice knows both Bob and Charlie
    database.transaction(() -> {
      final MutableVertex alice = database.query("opencypher", "MATCH (p:Person {name:'Alice'}) RETURN p").next()
          .getElement().orElseThrow().asVertex().modify();
      final MutableVertex charlie = database.query("opencypher", "MATCH (p:Person {name:'Charlie'}) RETURN p").next()
          .getElement().orElseThrow().asVertex().modify();
      alice.newEdge("KNOWS", charlie, new Object[0]).save();
    });

    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person {name:'Alice'}) \
        RETURN COLLECT { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name <> 'Bob' RETURN f.name } AS names""");

    assertThat(results.hasNext()).isTrue();
    final Result r = results.next();
    final Object names = r.getProperty("names");
    assertThat(names).isInstanceOf(List.class);
    @SuppressWarnings("unchecked") final List<Object> list = (List<Object>) names;
    assertThat(list).containsExactly("Charlie");
  }
}
