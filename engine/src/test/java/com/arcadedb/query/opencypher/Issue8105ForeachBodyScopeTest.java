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
 * Regression tests for issue #8105: {@code CypherSemanticValidator.validateVariableScope} skipped the
 * body of a {@code FOREACH} clause entirely, so a body writing through a variable a preceding
 * {@code WITH} had dropped - or one that was never bound at all - was accepted, did nothing, and
 * reported success instead of raising {@code UndefinedVariable} the way Neo4j does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8105ForeachBodyScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:Function {Name:'Zeta'})");
  }

  /** The reporter's query: `f` is dropped by the projecting WITH, then written to inside FOREACH's SET. */
  @Test
  void foreachSetReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (f:Function) \
        WITH f.Name AS BF \
        FOREACH (x IN [1] | SET f.Name = 'q') \
        RETURN BF""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /** Same shape, but the never-bound name is referenced inside FOREACH's REMOVE. */
  @Test
  void foreachRemoveReferencingUnboundVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (f:Function) \
        FOREACH (x IN [1] | REMOVE ghost.Name) \
        RETURN f.Name AS BusinessFunction""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'ghost'");
  }

  /** Same shape, but the out-of-scope reference is inside a FOREACH nested two levels deep. */
  @Test
  void nestedForeachReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (f:Function) \
        WITH f.Name AS BF \
        FOREACH (x IN [1] | FOREACH (y IN [1] | SET f.Name = 'q')) \
        RETURN BF""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'f'");
  }

  /** Control: a variable the FOREACH body can legitimately see still works, and the write takes effect. */
  @Test
  void foreachSetReferencingInScopeVariableSucceeds() {
    database.command("opencypher",
        """
        MATCH (f:Function) \
        FOREACH (x IN [1] | SET f.Name = 'Updated')""").close();

    try (final ResultSet rs = database.query("opencypher", "MATCH (f:Function) RETURN f.Name AS name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Updated");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Control: the FOREACH's own loop variable is in scope inside its body. */
  @Test
  void foreachLoopVariableIsInScopeInsideBody() {
    database.command("opencypher",
        "CREATE (:Counter {Value:0})").close();
    database.command("opencypher",
        """
        MATCH (c:Counter) \
        FOREACH (x IN [1,2,3] | SET c.Value = x)""").close();

    try (final ResultSet rs = database.query("opencypher", "MATCH (c:Counter) RETURN c.Value AS v")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(3);
    }
  }
}
