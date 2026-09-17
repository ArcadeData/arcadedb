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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7774: the grammar accepts {@code NULL modifier*} (e.g. {@code null.ifNull('default')}),
 * but {@code SQLASTBuilder#visitNullBaseExpr} never built the modifier chain, so the whole method/selector chain was
 * silently discarded and the query answered a bare {@code null}. A parenthesised literal carrying a modifier, e.g.
 * {@code (42).asString()}, had the same defect from the other side: {@code BaseExpression#rendersParentheses()}
 * dropped the written parentheses whenever the atom had no arithmetic operators, so the re-rendered text
 * ({@code 42.asString()}) became a syntax error, and {@code (null).ifNull('default')} re-rendered as
 * {@code NULL.ifNull('default')} - which, before the first fix, evaluated to a DIFFERENT (null) answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7774NullModifierChainTest extends TestHelper {

  @Test
  void bareNullHonoursItsModifierChain() {
    assertThat(scalar("SELECT null.ifNull('default') AS r")).isEqualTo("default");
    assertThat(scalar("SELECT null.type() AS r")).isNull();
    assertThat(scalar("SELECT null.asString() AS r")).isNull();
  }

  @Test
  void parenthesizedNullHonoursItsModifierChain() {
    assertThat(scalar("SELECT (null).ifNull('default') AS r")).isEqualTo("default");
  }

  @Test
  void reRenderingAParenthesizedNullModifierChainStaysReparseableAndKeepsTheSameAnswer() {
    final String rendered = renderStatement("SELECT (null).ifNull('default') AS r");
    // The written parentheses must survive the round trip: they carry the modifier chain now that a bare
    // NULL.ifNull(...) is itself meaningful, so dropping them must not change what the text means.
    assertThat(rendered).containsIgnoringCase("(null)");
    assertThat(scalar(rendered)).isEqualTo("default");
  }

  @Test
  void reRenderingAParenthesizedNumericLiteralModifierChainStaysReparseable() {
    // NUMBER has no bare `modifier*` grammar alternative of its own (only NULL does), so the parentheses are the
    // only thing standing between a valid statement and a syntax error once it is rendered back to text.
    final String rendered = renderStatement("SELECT (42).asString() AS r");
    assertThat(rendered).contains("(42)");
    assertThat(scalar(rendered)).isEqualTo("42");
  }

  private String renderStatement(final String query) {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final Statement statement = parser.parse(query);
    final StringBuilder builder = new StringBuilder();
    statement.toString(null, builder);
    return builder.toString();
  }

  private Object scalar(final String query) {
    try (final ResultSet rs = database.query("sql", query, Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return result.getProperty("r");
    }
  }
}
