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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5179: a variable introduced only inside a pattern comprehension
 * (or list comprehension) must not leak into the outer {@code any()/all()/none()/single()}
 * predicate. Referencing such a variable must raise an undefined-variable scope error, matching
 * Neo4j ({@code SyntaxError: Variable `n` not defined}) and Memgraph ({@code Unbound variable: n}),
 * instead of silently returning an empty result set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5179PatternComprehensionScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (a:A {v:1}), (b:B {v:10}), (a)-[:R]->(b)");
  }

  /** The reporter's failing query: pattern-comprehension-local {@code n} referenced in any() WHERE. */
  @Test
  void patternComprehensionVariableLeakInAnyPredicateThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:A) WHERE any(p IN [(a)-[:R]->(n) | n.v] WHERE n.v > 5) RETURN a.v").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("n");
  }

  /** Optional control from the issue: same leak with an ordinary list comprehension in RETURN. */
  @Test
  void listComprehensionVariableLeakInAnyPredicateThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "RETURN any(p IN [x IN [10] | x] WHERE x > 5) AS r").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("x");
  }

  /** Control 1: referencing the any() iteration variable {@code p} is valid and returns a.v = 1. */
  @Test
  void iterationVariableIsInScope() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A) WHERE any(p IN [(a)-[:R]->(n) | n.v] WHERE p > 5) RETURN a.v AS v");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("v")).intValue()).isEqualTo(1);
    assertThat(rs.hasNext()).isFalse();
  }

  /** Control 2: {@code n} used inside the pattern comprehension itself is valid and yields [10]. */
  @Test
  void patternComprehensionInternalUsageIsValid() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:A) RETURN [(a)-[:R]->(n) | n.v] AS vals");
    assertThat(rs.hasNext()).isTrue();
    final Object vals = rs.next().getProperty("vals");
    assertThat(vals).isInstanceOf(Iterable.class);
    @SuppressWarnings("unchecked")
    final Iterable<Object> list = (Iterable<Object>) vals;
    assertThat(list).containsExactly(10);
  }
}
