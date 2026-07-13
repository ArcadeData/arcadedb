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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5257: {@code CREATE} and {@code MERGE} patterns inside a {@code CALL}
 * subquery must obey the same variable-scope rules as {@code SET} and {@code DELETE}. Binding an outer
 * variable that was never imported used to silently mint fresh anonymous vertices, persist the edge
 * between those orphans, and still report {@code relationshipsCreated: 1}. Neo4j and Memgraph both
 * reject such queries.
 */
class Issue5257CallSubqueryRelationshipScopeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-issue5257").create();
    database.command("opencypher", "CREATE (:A {id: 1}), (:B {id: 2})");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The reporter's case 1: CREATE of a relationship between two unimported outer variables. */
  @Test
  void createRelationshipWithUnimportedOuterVariablesThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (a:A {id: 1}), (b:B {id: 2}) \
        CALL { \
          CREATE (a)-[:R]->(b) \
          RETURN 1 AS ok \
        } \
        RETURN ok"""))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("a");

    assertNoOrphanWrites("R");
  }

  /** The reporter's case 2: MERGE has the same false-success behaviour. */
  @Test
  void mergeRelationshipWithUnimportedOuterVariablesThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (a:A {id: 1}), (b:B {id: 2}) \
        CALL { \
          MERGE (a)-[:MR]->(b) \
          RETURN 1 AS ok \
        } \
        RETURN ok"""))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("a");

    assertNoOrphanWrites("MR");
  }

  /** A partially importing WITH must still flag the endpoint it did not import. */
  @Test
  void createRelationshipWithPartiallyImportedOuterVariablesThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (a:A {id: 1}), (b:B {id: 2}) \
        CALL { \
          WITH a \
          CREATE (a)-[:R]->(b) \
          RETURN 1 AS ok \
        } \
        RETURN ok"""))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("b");

    assertNoOrphanWrites("R");
  }

  /** A relationship variable bound in the outer scope but not imported must also be rejected. */
  @Test
  void createWithUnimportedOuterRelationshipVariableThrows() {
    database.command("opencypher", "MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:R]->(b)");

    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (a:A)-[r:R]->(b:B) \
        CALL { \
          CREATE (x:X)-[r:R2]->(y:Y) \
          RETURN 1 AS ok \
        } \
        RETURN ok"""))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("r");
  }

  /**
   * The guard is not limited to relationship patterns: a single-node CREATE re-binding an unimported
   * outer variable is the same shadowing violation and must be rejected too.
   */
  @Test
  void createSingleNodeWithUnimportedOuterVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (a:A {id: 1}) \
        CALL { \
          CREATE (a:Shadow) \
          RETURN 1 AS ok \
        } \
        RETURN ok"""))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("a");

    assertThat(countVertices()).isEqualTo(2);
  }

  /** Nested subqueries inherit the shadowed names of every enclosing scope. */
  @Test
  void nestedCallCreateRelationshipWithUnimportedOuterVariablesThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (a:A {id: 1}), (b:B {id: 2}) \
        CALL (a, b) { \
          CALL { \
            CREATE (a)-[:R]->(b) \
            RETURN 1 AS ok \
          } \
          RETURN ok \
        } \
        RETURN ok"""))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("UndefinedVariable");

    assertNoOrphanWrites("R");
  }

  /** Control: an importing WITH makes the relationship CREATE valid and it really persists. */
  @Test
  void createRelationshipWithImportingWithPersists() {
    final ResultSet rs = database.command("opencypher",
        """
        MATCH (a:A {id: 1}), (b:B {id: 2}) \
        CALL { \
          WITH a, b \
          CREATE (a)-[:CR]->(b) \
          RETURN 1 AS ok \
        } \
        RETURN ok""");
    assertThat(rs.hasNext()).isTrue();
    rs.close();

    assertThat(countRelationships("CR")).isEqualTo(1);
    assertThat(countVertices()).isEqualTo(2);
  }

  /** Control: an explicit scope list makes the relationship MERGE valid and it really persists. */
  @Test
  void mergeRelationshipWithExplicitScopeListPersists() {
    final ResultSet rs = database.command("opencypher",
        """
        MATCH (a:A {id: 1}), (b:B {id: 2}) \
        CALL (a, b) { \
          MERGE (a)-[:MR]->(b) \
          RETURN 1 AS ok \
        } \
        RETURN ok""");
    assertThat(rs.hasNext()).isTrue();
    rs.close();

    assertThat(countRelationships("MR")).isEqualTo(1);
    assertThat(countVertices()).isEqualTo(2);
  }

  /** Control: a subquery that declares its own pattern variables stays valid. */
  @Test
  void selfContainedCreateInsideSubqueryPersists() {
    final ResultSet rs = database.command("opencypher",
        """
        MATCH (a:A {id: 1}) \
        CALL { \
          CREATE (x:X {id: 10})-[:XR]->(y:Y {id: 11}) \
          RETURN 1 AS ok \
        } \
        RETURN ok""");
    assertThat(rs.hasNext()).isTrue();
    rs.close();

    assertThat(countRelationships("XR")).isEqualTo(1);
  }

  /** Control: top-level CREATE of a fresh relationship pattern is unaffected. */
  @Test
  void topLevelCreateRelationshipPersists() {
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:TR]->(b) RETURN 1 AS ok");
    assertThat(rs.hasNext()).isTrue();
    rs.close();

    assertThat(countRelationships("TR")).isEqualTo(1);
    assertThat(countVertices()).isEqualTo(2);
  }

  private void assertNoOrphanWrites(final String relType) {
    assertThat(countRelationships(relType)).isZero();
    assertThat(countVertices()).isEqualTo(2);
  }

  private long countRelationships(final String relType) {
    // An absent type is the strongest form of "nothing was created"; querying it would throw.
    if (!database.getSchema().existsType(relType))
      return 0;

    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()-[r:" + relType + "]->() RETURN count(r) AS cnt")) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : 0;
    }
  }

  private long countVertices() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN count(n) AS cnt")) {
      return ((Number) rs.next().getProperty("cnt")).longValue();
    }
  }
}
