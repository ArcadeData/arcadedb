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
 * Regression test for issue #7426: the {@code ORDER BY} of a plain (neither {@code DISTINCT} nor
 * aggregating) {@code RETURN} must resolve its variables against the scope that reaches the
 * {@code RETURN}, plus the columns that {@code RETURN} projects.
 * <p>
 * {@code MATCH (f:Function) WITH f.Name AS BusinessFunction RETURN BusinessFunction ORDER BY f.Name}
 * used to be accepted and to return the rows <i>unsorted</i>: the projecting {@code WITH} drops
 * {@code f}, the sort key resolved to nothing, and the client got wrong results with no error. The
 * collapsing forms of the same clause ({@code RETURN DISTINCT}, an aggregating {@code RETURN}) and
 * every form of {@code WITH} already reported it; the plain {@code RETURN} was the one shape left
 * without the check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7426PlainReturnOrderByScopeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue7426");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.command("cypher", "CREATE (:Function {Name: 'Zeta'})");
      database.command("cypher", "CREATE (:Function {Name: 'Alpha'})");
      database.command("cypher", "CREATE (:Function {Name: 'Mid'})");
    });
  }

  @AfterEach
  void tearDown() {
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
   * The reported query: the projecting WITH drops {@code f}, so the trailing ORDER BY has no {@code f}
   * to sort on and must be reported rather than silently ignored.
   */
  @Test
  void plainReturnOrderByOnVariableDroppedByWithIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN BusinessFunction
        ORDER BY f.Name""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * Same shape with the whole variable rather than a property of it.
   */
  @Test
  void plainReturnOrderByOnBareDroppedVariableIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN BusinessFunction
        ORDER BY f""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * A variable that no clause ever bound reaches the same check: before the fix this too sorted on
   * nothing instead of failing.
   */
  @Test
  void plainReturnOrderByOnNeverBoundVariableIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        RETURN f.Name AS BusinessFunction
        ORDER BY ghost.Name""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'ghost'");
  }

  /**
   * The dropped variable buried inside a function call is the same violation and must be found by the
   * recursive walk, not only at the top of the ORDER BY expression.
   */
  @Test
  void plainReturnOrderByOnDroppedVariableInsideExpressionIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN BusinessFunction
        ORDER BY toLower(f.Name)""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * A UNION branch is validated as its own statement, so the same plain-RETURN shape inside one must
   * report too.
   */
  @Test
  void plainReturnOrderByInsideUnionBranchIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN BusinessFunction
        ORDER BY f.Name
        UNION
        MATCH (g:Function)
        RETURN g.Name AS BusinessFunction""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * A CALL subquery body is validated by the same scope walk, so its own plain RETURN + ORDER BY is
   * held to the same rule.
   */
  @Test
  void plainReturnOrderByInsideCallSubqueryIsRejected() {
    assertThatThrownBy(() -> column("""
        CALL {
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction
          ORDER BY f.Name
        }
        RETURN BusinessFunction""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * A {@code COLLECT { }} body is handed the outer row as a seed and validated against the outer scope,
   * so its own plain RETURN + ORDER BY is held to the same rule as the top-level one.
   */
  @Test
  void plainReturnOrderByInsideCollectSubqueryExpressionIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN BusinessFunction, COLLECT { MATCH (g:Function) RETURN g.Name ORDER BY f.Name } AS Names""",
        "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * {@code RETURN *} projects the whole scope, so an ORDER BY naming something outside it is the same
   * violation as the aliased form.
   */
  @Test
  void returnStarOrderByOnDroppedVariableIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN *
        ORDER BY f.Name""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * {@code WITH *} narrows nothing, so the only way its ORDER BY can fail is by naming a variable no
   * clause ever bound. That branch returned before reaching any ORDER BY check, so it sorted on nothing
   * instead of reporting - the same defect as the reported one, found by the completeness sweep.
   */
  @Test
  void withStarOrderByOnNeverBoundVariableIsRejected() {
    assertThatThrownBy(() -> column("""
        MATCH (f:Function)
        WITH *
        ORDER BY ghost.Name
        RETURN f.Name AS BusinessFunction""", "BusinessFunction"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'ghost'");
  }

  // ---------------------------------------------------------------------------------------------
  // In-scope forms: all of these were already correct and must stay accepted and correctly sorted.
  // ---------------------------------------------------------------------------------------------

  @Test
  void orderByOnAVariableStillInScopeIsAccepted() {
    assertThat(column("MATCH (f:Function) RETURN f.Name AS BusinessFunction ORDER BY f.Name", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }

  @Test
  void orderByOnTheProjectedAliasIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN BusinessFunction
        ORDER BY BusinessFunction""", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }

  @Test
  void orderByOnAnAliasIntroducedByTheReturnItselfIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH f.Name AS Name
        RETURN Name AS BusinessFunction
        ORDER BY BusinessFunction DESC""", "BusinessFunction"))
        .containsExactly("Zeta", "Mid", "Alpha");
  }

  @Test
  void orderByOnAVariableTheWithCarriedForwardIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction, f
        RETURN BusinessFunction
        ORDER BY f.Name""", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }

  @Test
  void orderByOnAnExpressionOverInScopeVariablesIsAccepted() {
    assertThat(column("MATCH (f:Function) RETURN f.Name AS BusinessFunction ORDER BY toLower(f.Name) DESC", "BusinessFunction"))
        .containsExactly("Zeta", "Mid", "Alpha");
  }

  @Test
  void withStarOrderByOnAnInScopeVariableIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH *
        ORDER BY f.Name
        RETURN f.Name AS BusinessFunction""", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }

  @Test
  void withStarOrderByOnAnExtraAliasIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH *, f.Name AS BusinessFunction
        ORDER BY BusinessFunction DESC
        RETURN BusinessFunction""", "BusinessFunction"))
        .containsExactly("Zeta", "Mid", "Alpha");
  }

  @Test
  void returnStarOrderByOnAnInScopeVariableIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH f.Name AS BusinessFunction
        RETURN *
        ORDER BY BusinessFunction""", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }

  @Test
  void orderByAfterWithStarIsAccepted() {
    assertThat(column("""
        MATCH (f:Function)
        WITH *
        RETURN f.Name AS BusinessFunction
        ORDER BY f.Name""", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }

  @Test
  void orderByOnAnUnwoundVariableIsAccepted() {
    assertThat(column("UNWIND ['Zeta', 'Alpha', 'Mid'] AS x RETURN x AS BusinessFunction ORDER BY x", "BusinessFunction"))
        .containsExactly("Alpha", "Mid", "Zeta");
  }
}
