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
 * Regression tests for a gap CodeRabbit found reviewing #8105: {@code CypherSemanticValidator.checkExpressionScope}
 * had no branch for {@code ReduceExpression} or {@code AllReduceExpression}, so a variable dropped from scope (or
 * never bound) referenced inside {@code reduce()}/{@code allReduce()} was silently accepted anywhere the surrounding
 * expression was scope-checked - {@code RETURN}, {@code WITH}, and now also a {@code FOREACH} list expression since
 * #8105 routes it through the same walk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherReduceUndefinedVariableScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:Counter {values: [1,2,3]})");
  }

  @Test
  void reduceBodyReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH 0 AS y \
        RETURN reduce(total = 0, n IN [1,2,3] | total + v) AS r""").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  @Test
  void allReduceBodyReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
        UNWIND [0] AS v \
        WITH 0 AS y \
        RETURN allReduce(total = 0, n IN [1,2,3] | total + n, total < v) AS r""").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'v'");
  }

  /** Same gap, reached through a FOREACH list expression instead of RETURN (the path #8105 added). */
  @Test
  void foreachListExpressionReduceReferencingDroppedVariableThrows() {
    assertThatThrownBy(() -> database.command("opencypher",
        """
        MATCH (c:Counter) \
        WITH c.values AS vals \
        FOREACH (x IN reduce(total = [], n IN vals | total + missing) | SET c.touched = true)""").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'missing'");
  }

  /** Control: reduce()/allReduce() referencing only in-scope variables still works. */
  @Test
  void reduceAndAllReduceStillWorkWithInScopeVariables() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (c:Counter) RETURN reduce(total = 0, n IN c.values | total + n) AS sum, "
            + "allReduce(total = 0, n IN c.values | total + n, total < 100) AS ok")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Number>getProperty("sum").intValue()).isEqualTo(6);
      assertThat(row.<Boolean>getProperty("ok")).isTrue();
    }
  }
}
