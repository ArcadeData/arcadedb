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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5213: a nested {@code CALL} subquery that references a variable
 * from an outer scope without importing it (neither via a scope list {@code CALL (v) { ... }}
 * nor via an importing {@code WITH}) must be rejected with an undefined-variable scope error,
 * matching Neo4j ({@code Variable `b` not defined}) instead of silently binding the variable
 * to {@code null}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5213NestedCallScopeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-issue5213").create();
    database.command("opencypher", "CREATE (a:C1 {id: 1}), (b:C2 {v: 21}), (a)-[:R]->(b)");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * The reporter's failing query: the inner {@code CALL} references {@code b} from the enclosing
   * subquery without importing it. Neo4j raises "Variable `b` not defined".
   */
  @Test
  void nestedCallReferencingUnimportedOuterVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        MATCH (a:C1 {id: 1}) \
        CALL (a) { \
          MATCH (a)-[:R]->(b:C2) \
          CALL { \
            RETURN b.v * 2 AS doubled \
          } \
          RETURN doubled \
        } \
        RETURN doubled""").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("b");
  }

  /**
   * Top-level (non-nested) variant: a plain {@code CALL} referencing an unimported outer variable
   * must also be rejected.
   */
  @Test
  void topLevelCallReferencingUnimportedOuterVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:C1 {id: 1}) CALL { RETURN a.id AS x } RETURN x").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("a");
  }

  /** Control: importing the variable via the scope list makes the nested CALL valid. */
  @Test
  void nestedCallWithScopeImportIsValid() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:C1 {id: 1}) \
        CALL (a) { \
          MATCH (a)-[:R]->(b:C2) \
          CALL (b) { \
            RETURN b.v * 2 AS doubled \
          } \
          RETURN doubled \
        } \
        RETURN doubled""");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("doubled")).intValue()).isEqualTo(42);
    assertThat(rs.hasNext()).isFalse();
  }

  /** Control: importing the variable via an importing WITH makes the nested CALL valid. */
  @Test
  void nestedCallWithImportingWithIsValid() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:C1 {id: 1}) \
        CALL (a) { \
          MATCH (a)-[:R]->(b:C2) \
          CALL { \
            WITH b \
            RETURN b.v * 2 AS doubled \
          } \
          RETURN doubled \
        } \
        RETURN doubled""");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("doubled")).intValue()).isEqualTo(42);
    assertThat(rs.hasNext()).isFalse();
  }

  /** Control: a fully self-contained inner CALL (binds its own variables) stays valid. */
  @Test
  void selfContainedInnerCallIsValid() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:C1 {id: 1}) \
        CALL (a) { \
          MATCH (a)-[:R]->(b:C2) \
          CALL { \
            MATCH (c:C2) \
            RETURN c.v AS val \
          } \
          RETURN b.v + val AS total \
        } \
        RETURN total""");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("total")).intValue()).isEqualTo(42);
    assertThat(rs.hasNext()).isFalse();
  }
}
