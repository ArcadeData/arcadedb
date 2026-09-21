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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #8104: {@code CypherSemanticValidator.validateVariableScope} skipped
 * {@code REMOVE} entirely, so a {@code REMOVE} naming a variable a preceding {@code WITH} had dropped -
 * or one that no clause ever bound - was accepted, removed nothing, and reported success instead of
 * raising {@code UndefinedVariable} the way Neo4j does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8104RemoveScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:Function {Name:'Zeta'})");
  }

  /** The reporter's first query: `f` is dropped by the projecting WITH, then referenced by REMOVE. */
  @Test
  void removePropertyOnDroppedVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (f:Function) WITH f.Name AS BusinessFunction REMOVE f.Name RETURN BusinessFunction").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /** The reporter's second query: `ghost` was never bound by anything. */
  @Test
  void removePropertyOnNeverBoundVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (f:Function) REMOVE ghost.Name RETURN f.Name AS BusinessFunction").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'ghost'");
  }

  /** Same shape as the label-removal form: REMOVE n:Label on a dropped variable. */
  @Test
  void removeLabelOnDroppedVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (f:Function) WITH f.Name AS BusinessFunction REMOVE f:Function RETURN BusinessFunction").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /**
   * The dynamic-key form (REMOVE n[keyExpr]): the target variable `f` is in scope, but the key expression itself
   * references one that is not - the walk validateRemoveClauseScope gives item.getKeyExpression() (issue #8104).
   */
  @Test
  void removeDynamicPropertyKeyReferencingUnboundVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (f:Function) REMOVE f[missing]").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'missing'");
  }

  /**
   * The Cypher 25 dynamic-label form (REMOVE n:$(expr)): same shape, but for item.getLabelExpressions() instead of
   * the key expression.
   */
  @Test
  void removeDynamicLabelExpressionReferencingUnboundVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (f:Function) REMOVE f:$(missing)").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'missing'");
  }

  /** Control: REMOVE on a variable that is genuinely in scope still works and takes effect. */
  @Test
  void removePropertyOnInScopeVariableSucceeds() {
    database.command("opencypher", "MATCH (f:Function) REMOVE f.Name").close();

    try (final ResultSet rs = database.query("opencypher", "MATCH (f:Function) RETURN f.Name AS name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("name")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
