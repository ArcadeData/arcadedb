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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #3952: EXISTS { MATCH (p)-[:WORKS_WITH]->() } returned false for all rows because
 * the word-boundary check in ExistsExpression.matchesKeywordAt() did not treat underscore as an identifier
 * character, so "WITH" inside "WORKS_WITH" was falsely detected as the Cypher WITH clause keyword,
 * corrupting the injected subquery and causing a silently-caught exception.
 */
class CypherExistsUnderscoreRelationshipTypeTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherexistsunderscore").create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("WORKS_WITH");
      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().createEdgeType("KNOWS_WHERE");

      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
      final MutableVertex david = database.newVertex("Person").set("name", "David").save();

      // Alice -[WORKS_WITH]-> Bob  (type name embeds keyword "WITH")
      alice.newEdge("WORKS_WITH", bob, new Object[0]).save();
      // Charlie -[KNOWS]-> David   (simple type, control)
      charlie.newEdge("KNOWS", david, new Object[0]).save();
      // Alice -[KNOWS_WHERE]-> Charlie  (type name embeds keyword "WHERE")
      alice.newEdge("KNOWS_WHERE", charlie, new Object[0]).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * Regression for issue #3952: relationship type containing "_WITH" (a Cypher keyword after underscore)
   * must not break the EXISTS { MATCH ... } subquery injection.
   */
  @Test
  void existsWithUnderscoreKeywordWithInRelationshipType() {
    final ResultSet results = database.query("opencypher", """
        MATCH (p:Person)
        RETURN p.name AS person,
        EXISTS { MATCH (p)-[:WORKS_WITH]->(:Person) } AS worksWith
        ORDER BY person""");

    final Map<String, Boolean> actual = collectBooleanColumn(results, "person", "worksWith");

    assertThat(actual).containsEntry("Alice", true);
    assertThat(actual).containsEntry("Bob", false);
    assertThat(actual).containsEntry("Charlie", false);
    assertThat(actual).containsEntry("David", false);
  }

  /**
   * EXISTS with relationship type "KNOWS_WHERE" whose name embeds the keyword "WHERE".
   */
  @Test
  void existsWithUnderscoreKeywordWhereInRelationshipType() {
    final ResultSet results = database.query("opencypher", """
        MATCH (p:Person)
        RETURN p.name AS person,
        EXISTS { MATCH (p)-[:KNOWS_WHERE]->(:Person) } AS knowsWhere
        ORDER BY person""");

    final Map<String, Boolean> actual = collectBooleanColumn(results, "person", "knowsWhere");

    assertThat(actual).containsEntry("Alice", true);
    assertThat(actual).containsEntry("Bob", false);
    assertThat(actual).containsEntry("Charlie", false);
    assertThat(actual).containsEntry("David", false);
  }

  /**
   * Control: EXISTS with a simple relationship type (no embedded keyword) continues to work correctly.
   */
  @Test
  void existsWithSimpleRelationshipTypeStillWorks() {
    final ResultSet results = database.query("opencypher", """
        MATCH (p:Person)
        RETURN p.name AS person,
        EXISTS { MATCH (p)-[:KNOWS]->(:Person) } AS knows
        ORDER BY person""");

    final Map<String, Boolean> actual = collectBooleanColumn(results, "person", "knows");

    assertThat(actual).containsEntry("Alice", false);
    assertThat(actual).containsEntry("Bob", false);
    assertThat(actual).containsEntry("Charlie", true);
    assertThat(actual).containsEntry("David", false);
  }

  private static Map<String, Boolean> collectBooleanColumn(final ResultSet results, final String keyCol,
      final String valueCol) {
    final Map<String, Boolean> map = new HashMap<>();
    while (results.hasNext()) {
      final Result row = results.next();
      map.put(row.getProperty(keyCol), row.getProperty(valueCol));
    }
    results.close();
    return map;
  }
}
