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
 * Regression tests for issue #5825: a variable dropped from scope by a {@code WITH} clause is
 * correctly rejected when referenced directly (e.g. in {@code RETURN}), but was silently accepted
 * when referenced inside an {@code EXISTS { ... }}, {@code COUNT { ... }} or {@code COLLECT { ... }}
 * subquery, where it just resolved as missing/null instead of raising an undefined-variable error -
 * turning a scope mistake into a silently-empty or silently-wrong result instead of a client error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5825ExistsSubqueryScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:_Oracle {_id:'n1', p:0}), (:_Oracle {_id:'n2', p:1})");
  }

  /** The reporter's failing query: `v` is dropped by the second WITH, then referenced inside EXISTS. */
  @Test
  void existsSubqueryReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH v \
        WITH 0 AS y \
        MATCH (n) \
        WHERE EXISTS { \
          MATCH (x) \
          WHERE x.p = v \
        } \
        RETURN n._id AS n, y""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  /** Same shape, but the out-of-scope reference is inside COUNT { ... }. */
  @Test
  void countSubqueryReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH v \
        WITH 0 AS y \
        MATCH (n) \
        WHERE COUNT { \
          MATCH (x) \
          WHERE x.p = v \
        } > 0 \
        RETURN n._id AS n, y""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  /** Same shape, but the out-of-scope reference is inside COLLECT { ... }. */
  @Test
  void collectSubqueryReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH v \
        WITH 0 AS y \
        MATCH (n) \
        RETURN n._id AS n, y, COLLECT { \
          MATCH (x) \
          WHERE x.p = v \
          RETURN x._id \
        } AS c""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  /** Control: the same out-of-scope reference is already rejected when it is a direct RETURN. */
  @Test
  void directReferenceToDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH v \
        WITH 0 AS y \
        RETURN v""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  /** Same shape, but the out-of-scope reference is inside a UNION branch of an EXISTS subquery body. */
  @Test
  void existsSubqueryUnionBranchReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH v \
        WITH 0 AS y \
        MATCH (n) \
        WHERE EXISTS { \
          MATCH (x) \
          WHERE x.p = v \
          RETURN x \
          UNION \
          MATCH (z) \
          WHERE z.p = 1 \
          RETURN z \
        } \
        RETURN n._id AS n, y""").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  /** Control: keeping `v` in scope makes the EXISTS query valid and produces the expected rows. */
  @Test
  void existsSubqueryReferencingPreservedVariableIsValid() {
    final ResultSet rs = database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH v \
        WITH v, 0 AS y \
        MATCH (n) \
        WHERE EXISTS { \
          MATCH (x) \
          WHERE x.p = v \
        } \
        RETURN n._id AS n, y \
        ORDER BY n""");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("n")).isEqualTo("n1");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("n")).isEqualTo("n2");
    assertThat(rs.hasNext()).isFalse();
  }
}
