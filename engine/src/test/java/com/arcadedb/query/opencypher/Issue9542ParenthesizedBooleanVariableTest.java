/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #9542: {@code WHERE (flag)} with a Boolean variable was parsed as the single-node pattern
 * {@code (flag)} and silently filtered every row, while {@code WHERE flag} kept them. A parenthesized variable is a
 * pattern only if it names a graph entity; one bound to a value is just a parenthesized expression.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9542ParenthesizedBooleanVariableTest extends TestHelper {

  private List<Object> column(final String query, final String column) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty(column));
      }
    }
    return out;
  }

  @Test
  void parenthesizedBooleanVariableAfterWith() {
    assertThat(column("WITH true AS flag WHERE (flag) RETURN flag", "flag")).containsExactly(true);
    assertThat(column("WITH false AS flag WHERE (flag) RETURN flag", "flag")).isEmpty();
    assertThat(column("WITH true AS flag WHERE flag RETURN flag", "flag")).containsExactly(true);
  }

  @Test
  void parenthesizedComputedBooleanVariable() {
    assertThat(column("WITH 3.14 AS lhs, 1 AS rhs WITH lhs, rhs, lhs > rhs AS result WHERE (result) RETURN lhs", "lhs"))
        .containsExactly(3.14);
    assertThat(column("WITH 0.5 AS lhs, 1 AS rhs WITH lhs, rhs, lhs > rhs AS result WHERE (result) RETURN lhs", "lhs"))
        .isEmpty();
  }

  @Test
  void parenthesizedVariableInsideLogicalOperators() {
    assertThat(column("WITH true AS a, false AS b WHERE (a) AND NOT (b) RETURN a", "a")).containsExactly(true);
    assertThat(column("WITH true AS a, false AS b WHERE (b) OR (a) RETURN a", "a")).containsExactly(true);
    assertThat(column("WITH true AS a, false AS b WHERE (a) AND (b) RETURN a", "a")).isEmpty();
    assertThat(column("WITH true AS a WHERE NOT (a) RETURN a", "a")).isEmpty();
    assertThat(column("WITH false AS a WHERE NOT (a) RETURN a", "a")).containsExactly(false);
  }

  @Test
  void parenthesizedNullVariableIsUnknown() {
    assertThat(column("WITH null AS a WHERE (a) RETURN 1 AS x", "x")).isEmpty();
    // NOT of unknown is unknown, so the row is filtered
    assertThat(column("WITH null AS a WHERE NOT (a) RETURN 1 AS x", "x")).isEmpty();
    assertThat(column("WITH null AS a WHERE (a) OR true RETURN 1 AS x", "x")).containsExactly(1L);
  }

  @Test
  void parenthesizedBooleanVariableInMatchWhere() {
    database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1, ok: true}), (:N {id: 2, ok: false})"));
    assertThat(column("MATCH (n:N) WITH n, n.ok AS ok WHERE (ok) RETURN n.id AS id", "id")).containsExactly(1);
    assertThat(column("MATCH (n:N) WITH n, n.ok AS ok WHERE NOT (ok) RETURN n.id AS id", "id")).containsExactly(2);
    assertThat(column("UNWIND [true, false] AS ok MATCH (n:N) WHERE (ok) AND n.id = 1 RETURN n.id AS id", "id"))
        .containsExactly(1);
  }

  @Test
  void parenthesizedNodeVariableIsStillRefused() {
    database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})-[:R]->(:N {id: 2})"));
    assertThatThrownBy(() -> column("MATCH (n:N) WHERE (n) RETURN n.id AS id", "id")).isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> column("MATCH (n:N) WITH n WHERE (n) RETURN n.id AS id", "id"))
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> column("MATCH (n:N)-[r:R]->() WHERE (r) RETURN n.id AS id", "id"))
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> column("MATCH p = (n:N)-[:R]->() WHERE (p) RETURN n.id AS id", "id"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void parenthesizedUndefinedVariableIsRefused() {
    assertThatThrownBy(() -> column("MATCH (m) WHERE (nope) RETURN m", "m")).isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void parenthesizedNonBooleanValueIsATypeError() {
    // a number is not a Boolean: it is reported, not silently turned into "no row" by the single-node pattern reading
    assertThatThrownBy(() -> column("WITH 1 AS x WHERE (x) RETURN x", "x")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("InvalidArgumentType");
  }
}
