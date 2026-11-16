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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ModifierChainTest {

  /**
   * Regression for #7774: NULL and parenthesised literals keep their modifier chain and re-render parseably.
   */
  @Nested
  class NullModifierChain extends TestHelper {

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

  /**
   * Regression for #7896: redundant parentheses do not rename an unaliased projection column.
   */
  @Nested
  class UnaliasedParenthesizedProjectionName extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("T7896");
      database.transaction(() -> database.newDocument("T7896").set("name", "bob").set("tags", List.of("a", "b")).save());
    }

    @Test
    void redundantParenthesesAroundAnIdentifierDoNotRenameTheColumn() {
      assertThat(columnOf("SELECT (name).asString() FROM T7896")).isEqualTo("name.asString()");
      assertThat(valueOf("SELECT (name).asString() FROM T7896", "name.asString()")).isEqualTo("bob");
    }

    @Test
    void theParenthesisedAndTheBareFormAgreeOnTheName() {
      assertThat(columnOf("SELECT (name).asString() FROM T7896")).isEqualTo(columnOf("SELECT name.asString() FROM T7896"));
      assertThat(columnOf("SELECT (tags)[0] FROM T7896")).isEqualTo(columnOf("SELECT tags[0] FROM T7896"));
    }

    @Test
    void redundantParenthesesAroundAStringLiteralOrAFunctionCallDoNotRenameTheColumn() {
      assertThat(columnOf("SELECT ('a').append('b') FROM T7896")).isEqualTo("'a'.append('b')");
      assertThat(columnOf("SELECT (uuid()).asString() FROM T7896")).isEqualTo("uuid().asString()");
    }

    @Test
    void redundantParenthesesAroundABlockLiteralDoNotRenameTheColumn() {
      assertThat(columnOf("SELECT ([1,2,3]).size() FROM T7896")).doesNotStartWith("(");
      assertThat(valueOf("SELECT ([1,2,3]).size() FROM T7896", columnOf("SELECT ([1,2,3]).size() FROM T7896"))).isEqualTo(3);
    }

    @Test
    void aParenthesisedAtomWithNoModifierTailOfItsOwnKeepsItsParentheses() {
      // The cases issue #7774 is about: without the parentheses the rendered text is no longer parseable, so the
      // column name is the lesser of the two evils there.
      assertThat(columnOf("SELECT (42).asString() FROM T7896")).isEqualTo("(42).asString()");
      assertThat(columnOf("SELECT (null).ifNull('d') FROM T7896")).isEqualTo("(NULL).ifNull('d')");
      assertThat(valueOf("SELECT (42).asString() FROM T7896", "(42).asString()")).isEqualTo("42");
      assertThat(valueOf("SELECT (null).ifNull('d') FROM T7896", "(NULL).ifNull('d')")).isEqualTo("d");
    }

    @Test
    void redundantParenthesesAroundASubqueryDoNotRenameTheColumn() {
      // The grammar's `LPAREN statement RPAREN modifier*` alternative, reached through the two BaseExpression
      // subclasses that render their own parentheses. A second, WRITTEN pair around one of those is the redundant
      // kind, so it must not survive into the column name either (PR #8091 review asked whether that override was
      // reachable at all - it is, and this is the path).
      assertThat(renderOf("SELECT ((SELECT 1 AS v)).v FROM T7896")).isEqualTo("SELECT (SELECT 1 AS v).v FROM T7896");
      assertThat(renderOf("SELECT ((SELECT FROM T7896))[0] FROM T7896"))
          .isEqualTo("SELECT (SELECT FROM T7896)[0] FROM T7896");
    }

    @Test
    void parenthesesAroundACompoundArithmeticExpressionStillSurvive() {
      // Unchanged by this fix and by #7774 before it: dropping these re-associates the operators (issue #6359).
      assertThat(valueOf("SELECT (1 + 2) * 3 AS r FROM T7896", "r")).isEqualTo(9);
      assertThat(columnOf("SELECT (1 + 2) * 3 FROM T7896")).contains("(1 + 2)");
    }

    @Test
    void parenthesesWithNoModifierAtAllAreStillDropped() {
      assertThat(columnOf("SELECT (name) FROM T7896")).isEqualTo("name");
    }

    /** The text a parsed statement renders back to, which is what an unaliased projection is named after. */
    private String renderOf(final String query) {
      final StringBuilder builder = new StringBuilder();
      new SQLAntlrParser(null).parse(query).toString(null, builder);
      return builder.toString();
    }

    /** The name of the single (unaliased) column the query projects. */
    private String columnOf(final String query) {
      try (final ResultSet rs = database.query("sql", query, Map.of())) {
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        assertThat(result.getPropertyNames()).hasSize(1);
        return result.getPropertyNames().iterator().next();
      }
    }

    private Object valueOf(final String query, final String columnName) {
      try (final ResultSet rs = database.query("sql", query, Map.of())) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next().getProperty(columnName);
      }
    }
  }
}
