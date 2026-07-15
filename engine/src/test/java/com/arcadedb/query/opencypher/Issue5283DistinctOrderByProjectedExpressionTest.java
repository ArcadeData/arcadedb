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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5283: {@code ORDER BY} must be able to resolve an expression that is
 * itself one of the projected expressions of a {@code RETURN DISTINCT} / {@code WITH DISTINCT},
 * even when that projection carries an alias.
 * <p>
 * The reported query
 * <pre>
 *   MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active
 * </pre>
 * failed with {@code UndefinedVariable: Variable 'n' not defined}, while the equivalent
 * {@code ORDER BY active} worked.
 * <p>
 * A DISTINCT projection narrows the scope available to ORDER BY down to the projected columns, which
 * is why {@code ORDER BY <non-projected expression>} must keep failing (a dedup group has no single
 * well-defined value for it). But an ORDER BY expression that is <em>structurally identical</em> to a
 * projected expression is constant within every dedup group by construction, so it is well defined.
 * Neo4j, Memgraph and FalkorDB all accept it. Neo4j implements this by rewriting such an ORDER BY
 * expression to the projection's alias before the scope check; this test pins that same behaviour.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5283DistinctOrderByProjectedExpressionTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5283");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.command("cypher", "CREATE (:BugA {active: true, age: 30, name: 'c'})");
      database.command("cypher", "CREATE (:BugA {active: false, age: 20, name: 'b'})");
      database.command("cypher", "CREATE (:BugA {active: true, age: 10, name: 'a'})");
    });
  }

  @AfterEach
  public void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private List<Object> column(final String cypher, final String columnName) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty(columnName));
      }
    }
    return values;
  }

  /**
   * The exact query from the issue report. Must return the two distinct values ascending.
   */
  @Test
  public void orderByAliasedProjectedExpression() {
    assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active", "active")) //
        .containsExactly(false, true);
  }

  /**
   * Control from the issue report: ordering by the alias already worked and must keep working.
   */
  @Test
  public void orderByAliasStillWorks() {
    assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY active", "active")) //
        .containsExactly(false, true);
  }

  /**
   * The rewrite must honour the sort direction of the ORDER BY item, not the projection order.
   */
  @Test
  public void orderByAliasedProjectedExpressionDescending() {
    assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active DESC", "active")) //
        .containsExactly(true, false);
  }

  /**
   * Un-aliased projections resolve through the same path: the output column is the expression text.
   */
  @Test
  public void orderByProjectedExpressionWithoutAlias() {
    assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.age ORDER BY n.age", "n.age")) //
        .containsExactly(10, 20, 30);
  }

  /**
   * Composite expressions must match structurally, so that whitespace differences between the
   * projection text and the ORDER BY text cannot cause a spurious UndefinedVariable.
   */
  @Test
  public void orderByProjectedCompositeExpression() {
    // Cypher integer arithmetic widens to 64-bit, hence Long rather than Integer
    assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.age + 1 AS bumped ORDER BY n.age+1", "bumped")) //
        .containsExactly(11L, 21L, 31L);
  }

  /**
   * Same rule applies to WITH DISTINCT, whose ORDER BY is subject to the same narrowed scope.
   */
  @Test
  public void withDistinctOrderByProjectedExpression() {
    assertThat(column("MATCH (n:BugA) WITH DISTINCT n.age AS age ORDER BY n.age RETURN age", "age")) //
        .containsExactly(10, 20, 30);
  }

  /**
   * Neo4j-compatible negative case: {@code n.name} is not projected, so after DISTINCT collapses
   * {active:true} down to one row there is no well-defined {@code n.name} to sort on. This must stay
   * an error - relaxing the scope check must not become "let any pre-DISTINCT variable through".
   */
  @Test
  public void orderByNonProjectedExpressionStillRejected() {
    assertThatThrownBy(() -> database.query("cypher", "MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.name").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("not defined");
  }

  /**
   * A projected expression only licenses that exact expression: sorting by a different property of
   * the same variable stays rejected.
   */
  @Test
  public void orderByDifferentPropertyOfProjectedVariableStillRejected() {
    assertThatThrownBy(() -> database.query("cypher", "MATCH (n:BugA) RETURN DISTINCT n.age AS age ORDER BY n.active").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("not defined");
  }
}
